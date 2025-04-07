import asyncio
import aiofiles
import io
from pathlib import Path
from typing import AsyncGenerator
from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup

async def generate_links(file_path: Path) -> AsyncGenerator[str, None]:
    """
    Asynchronously reads a WARC file, parses HTML content,
    and yields links one by one.
    """
    # Read file (gzipped WARC) asynchronously into memory.
    # For very large files, consider a streaming approach, but warcio
    # typically expects a synchronous file-like object in one piece.
    async with aiofiles.open(file_path, 'rb') as afp:
        warc_bytes = await afp.read()

    # Convert the bytes to a BytesIO for warcio's ArchiveIterator
    max_links = 1000
    count = 0
    with io.BytesIO(warc_bytes) as warc_stream:
        for record in ArchiveIterator(warc_stream):
            if record.rec_type == 'response':
                content_type = record.http_headers.get('Content-Type', '')
                source_url = record.rec_headers.get('WARC-Target-URI', '')
                if 'text/html' in content_type.lower():
                    html_content = record.content_stream().read()
                    html_str = html_content.decode('utf-8', errors='replace')

                    soup = BeautifulSoup(html_str, 'html.parser')
                    for a_tag in soup.find_all('a', href=True):
                        is_valid_link = a_tag['href'].startswith(('http://', 'https://')) and not a_tag['href'] in {'http://', 'https://'}
                        if is_valid_link:
                            count += 1
                            yield (source_url, a_tag['href'])
                            if count >= max_links:
                                return

async def read_warc_and_enqueue(file_path: Path, queue: asyncio.Queue):
    """
    Reads links from a single WARC file (async generator),
    and puts them on the queue for the writer to consume.
    """
    async for url_link_tuple in generate_links(file_path):
        # Put each link on the queue as soon as we find it
        await queue.put(url_link_tuple)

async def write_links_to_file(queue: asyncio.Queue, output_file: Path):
    """
    Continuously reads links from the queue and writes them to 'output_file'.
    This runs until cancelled (when all producers are finished and queue is empty).
    """
    print(f"Printer task started, writing to {output_file}")
    async with aiofiles.open(output_file, 'w') as afp:
        while True:
            source_url, link = await queue.get()  # blocks until an item is available
            await afp.write(f"{source_url},{link}" + "\n")
            queue.task_done()

async def main(segments_folder: str | Path):
    segments_folder = Path(segments_folder)
    warc_files = list(segments_folder.glob("*.warc.gz"))
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
        tasks.append(asyncio.create_task(read_warc_and_enqueue(file_path, queue)))

    # Wait for all WARC parsing tasks to finish
    await asyncio.gather(*tasks)

    # Wait for the queue to be fully processed (all links written)
    await queue.join()

    # Cancel the writer task (it has a while True loop)
    writer_task.cancel()

    print(f"Links successfully written to {output_file}")

if __name__ == "__main__":
    asyncio.run(main("./commoncrawl/segments/"))
