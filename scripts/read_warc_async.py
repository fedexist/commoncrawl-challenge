import asyncio
import os
import aiofiles
import io
from pathlib import Path
from typing import AsyncGenerator
from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup
import urllib.parse


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
    file_path: Path, max_links_per_file: int, queue: asyncio.Queue
):
    """
    Reads links from a single WARC file (async generator),
    and puts them on the queue for the writer to consume.
    """
    async for link in generate_links(file_path, max_links_per_file):
        # Put each link on the queue as soon as we find it
        await queue.put(link)


async def write_links_to_file(queue: asyncio.Queue, output_file: Path):
    """
    Continuously reads links from the queue and writes them to 'output_file'.
    This runs until cancelled (when all producers are finished and queue is empty).
    """
    print(f"Printer task started, writing to {output_file}")
    async with aiofiles.open(output_file, "w") as afp:
        while True:
            link = await queue.get()  # blocks until an item is available

            if link == "":
                queue.task_done()

            link = urllib.parse.quote_plus(link)  # URL-encode the link

            if '"' in link:
                link = link.replace('"', "%22")
            if "," in link:
                link = f'"{link}"'  # Escape commas in links

            await afp.write(link + "\n")
            queue.task_done()


async def main(segments_folder: str | Path):
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

    # Create a writer task (consumer)
    writer_task = asyncio.create_task(write_links_to_file(queue, output_file))

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

    # Wait for the queue to be fully processed (all links written)
    await queue.join()

    # Cancel the writer task (it has a while True loop)
    writer_task.cancel()

    print(f"Links successfully written to {output_file}")


if __name__ == "__main__":
    segments_folder = os.getenv("SEGMENTS_FOLDER")
    if not segments_folder:
        print("Please set the SEGMENTS_FOLDER environment variable.")
        exit(1)
    asyncio.run(main(segments_folder))
