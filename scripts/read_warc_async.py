import asyncio
import os
import aiofiles
import io
from pathlib import Path
from typing import AsyncGenerator
from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup

from utils import clean_link, encode_url_path_only

SHUTDOWN = object()

async def generate_links(
    file_path: Path, limit: int = None
) -> AsyncGenerator[str, None]:
    """
    Asynchronously reads a WARC file, parses HTML content,
    and yields links one by one.
    """
    # Read file (gzipped WARC) asynchronously into memory.
    # For very large files, consider a streaming approach, but warcio
    # typically expects a synchronous file-like object in one piece.
    async with aiofiles.open(file_path, "rb") as afp:
        warc_bytes = await afp.read()

    # Convert the bytes to a BytesIO for warcio's ArchiveIterator
    count = 0
    with io.BytesIO(warc_bytes) as warc_stream:
        for record in ArchiveIterator(warc_stream):
            if record.rec_type == "response":
                content_type = record.http_headers.get("Content-Type", "")
                source_url = record.rec_headers.get("WARC-Target-URI", "")
                yield source_url
                if "text/html" in content_type.lower():
                    html_content = record.content_stream().read()
                    html_str = html_content.decode("utf-8", errors="replace")

                    soup = BeautifulSoup(html_str, "html.parser")
                    for a_tag in soup.find_all("a", href=True):
                        is_valid_link = a_tag["href"].startswith(
                            ("http://", "https://")
                        ) and a_tag["href"] not in {"http://", "https://"}
                        if is_valid_link:
                            count += 1
                            yield a_tag["href"]
                            if limit and count >= limit:
                                return


async def read_warc_and_enqueue(
    file_path: Path,
    max_links_per_file: int,
    queue: asyncio.Queue,
    batch_size: int = 10000,
):
    print(f"📥 Start reading {file_path.name}")
    buffer = []
    count = 0

    async for link in generate_links(file_path, max_links_per_file):
        buffer.append(link)
        count += 1

        if len(buffer) >= batch_size:
            await queue.put(buffer.copy())  # put batch
            buffer.clear()

    if buffer:
        await queue.put(buffer.copy())

    print(f"✅ Enqueued {count} links from {file_path.name}")


async def write_links_to_file(
    queue: asyncio.Queue,
    output_file: Path,
    writer_id: int,
    flush_interval: float = 10.0
):
    """
    Each writer reads links from the queue and writes them in batches to its own 'output_file'.
    Links are URL-encoded and comma-escaped. Writers exit cleanly on SHUTDOWN sentinel.
    """
    print(f"📝 Writer {writer_id} started, writing to {output_file}")
    buffer = []
    last_flush = asyncio.get_event_loop().time()

    async with aiofiles.open(output_file, 'w') as afp:
        while True:
            try:
                await asyncio.sleep(0.01)
                # Try to get a new link with timeout to trigger flush
                link_batch = await asyncio.wait_for(queue.get(), timeout=flush_interval)
            except asyncio.TimeoutError:
                # Time-based flush (if idle)
                if buffer:
                    await afp.writelines(buffer)
                    await afp.flush()
                    print(f"🌀 Writer {writer_id}: Flushed {len(buffer)} links (timeout)")
                    buffer.clear()
                    last_flush = asyncio.get_event_loop().time()
                continue

            try:
                if link_batch is SHUTDOWN:
                    # Final flush before shutdown
                    if buffer:
                        await afp.writelines(buffer)
                        await afp.flush()
                        print(f"✅ Writer {writer_id}: Final flush of {len(buffer)} links on shutdown")
                        buffer.clear()
                    print(f"🛑 Writer {writer_id} exiting.")
                    break

                # Process link
                cleaned_links = [clean_link(link) for link in link_batch]
                links_from_batch = [link + "\n" for link in cleaned_links if link]
                buffer.extend(links_from_batch)

                # Batch flush
                now = asyncio.get_event_loop().time()
                if len(buffer) >= 100 or (now - last_flush) >= flush_interval:
                    await afp.writelines(buffer)
                    await afp.flush()
                    # print(f"🚀 Writer {writer_id}: Flushed {len(buffer)} links")
                    buffer.clear()
                    last_flush = now

            finally:
                queue.task_done()

async def main(segments_folder: str | Path):
    
    NUM_WRITERS = min(32, os.cpu_count() + 4)
    segments_folder = Path(segments_folder)
    warc_files = list(segments_folder.glob("*.warc.gz"))

    max_links_per_file = os.getenv("MAX_LINKS_PER_FILE")
    if max_links_per_file:
        try:
            max_links_per_file = int(max_links_per_file)
        except ValueError:
            print("Invalid value for MAX_LINKS_PER_FILE. It should be an integer.")
            return
    else:
        max_links_per_file = None
    print(f"Max links per file: {max_links_per_file}")

    if not warc_files:
        print("No WARC files found in the specified directory.")
        return

    print(f"Found {len(warc_files)} WARC files.")

    # Output file for all links
    output_file = segments_folder / "extracted_links.txt"

    # Create a queue to pass links from multiple WARC readers to the file writer
    queue = asyncio.Queue()

    # Create n writer tasks (consumers)
    writer_tasks = [
        asyncio.create_task(write_links_to_file(queue, segments_folder / f"extracted_links.{i}.txt", i))
        for i in range(NUM_WRITERS)
    ]
    
    # Create producer tasks to parse each WARC file in parallel
    tasks = []
    for file_path in warc_files:
        tasks.append(
            asyncio.create_task(
                read_warc_and_enqueue(file_path, max_links_per_file, queue)
            )
        )

    # Wait for all WARC parsing tasks to finish
    await asyncio.gather(*tasks)
    print("✅ All WARC parsing tasks finished.")

    # Wait for the queue to be fully processed
    print(f"📦 Queue size before join: {queue.qsize()}")
    await queue.join()
    print("✅ Queue fully drained")
    
    # Send one shutdown signal per writer
    for _ in range(NUM_WRITERS):
        await queue.put(SHUTDOWN)
    print("✅ Sentinels sent to writers.")

    # Wait for all writers to exit
    await asyncio.gather(*writer_tasks)
    print("✅ All writers exited.")
    
    # Join the output files into one
    print(f"📦 Merging output files into {output_file}")
    
    async with aiofiles.open(output_file, 'w') as afp:
        for i in range(NUM_WRITERS):
            writer_file = segments_folder / f"extracted_links.{i}.txt"
            async with aiofiles.open(writer_file, 'r') as writer_afp:
                async for line in writer_afp:
                    await afp.write(line)
            os.remove(writer_file)
    print("✅ Merged all output files.")
    print(f"✅ All links written to {output_file}")
    

if __name__ == "__main__":
    segments_folder = os.getenv("SEGMENTS_FOLDER")
    if not segments_folder:
        print("Please set the SEGMENTS_FOLDER environment variable.")
        exit(1)
    asyncio.run(main(segments_folder))
