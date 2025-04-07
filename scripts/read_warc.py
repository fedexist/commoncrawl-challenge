from warcio.archiveiterator import ArchiveIterator
import aiofiles
from bs4 import BeautifulSoup
import asyncio
from pathlib import Path

async def read_warc_file(file_path: str) -> list[str]:
    extracted_links: list[str] = []
    with open(file_path, 'rb') as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == 'response':
                content_type: str = record.http_headers.get('Content-Type', '')
                if 'text/html' in content_type.lower():
                    html_content = record.content_stream().read()
                    # Convert bytes to string (utf-8 or detect encoding if necessary)
                    html_str = html_content.decode('utf-8', errors='replace')

                    # 4. Use BeautifulSoup to extract links
                    soup = BeautifulSoup(html_str, 'html.parser')
                    links = []
                    for a_tag in soup.find_all('a', href=True):
                        links.append(a_tag['href'])

                    extracted_links.extend(list(set(links)))
    
    return extracted_links

async def main(segments_folder: str | Path):
    # 1. Get the list of WARC files
    segments_folder = Path(segments_folder)
    warc_files = list(segments_folder.glob("*.warc.gz"))
    if not warc_files:
        print("No WARC files found in the specified directory.")
        return
    print(f"Found {len(warc_files)} WARC files.")
    extracted_data = []
    # 2. Read each WARC file and extract links
    for file_path in warc_files:
        print(f"Processing {file_path}...")
        links = await read_warc_file(file_path)
        extracted_data.extend(links)
    
    # 3. Save the extracted links to a file
    output_file = segments_folder / "extracted_links.txt"
    async with aiofiles.open(output_file, 'w') as f:
        for link in extracted_data:
            await f.write(link + '\n')
    print(f"Extracted links saved to {output_file}")
    

if __name__ == "__main__":
    file_path = "./commoncrawl/segments/"
    asyncio.run(main(file_path))