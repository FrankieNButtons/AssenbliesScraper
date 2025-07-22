import time;
import os;
from selenium import webdriver;
from selenium.webdriver.chrome.service import Service;
from selenium.webdriver.chrome.options import Options;
from selenium.webdriver.common.by import By;
from get_user_agent import get_user_agent_of_pc;
from selenium.webdriver.support.wait import WebDriverWait;
from selenium.webdriver.support import expected_conditions as EC;
import requests;
from concurrent.futures import ThreadPoolExecutor, as_completed;
from tqdm import tqdm

URL = "https://data.humanpangenome.org/assemblies";
def getfileURLs(driver: webdriver.Edge, fileCollection: dict) -> dict:
    """
    **Description**  
    Locate all file rows and their corresponding download links on the page,  
    then store them in the fileCollection dictionary.

    **Params**  
    - `driver`: the Selenium Edge driver instance  
    - `fileCollection`: dictionary to store index-URL mappings

    **Returns**  
    - Updated fileCollection dictionary
    """
    
    fileElems = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, '//tr[@class="MuiTableRow-root css-1bun0x4"]')));
    fileIndes = [fileElem.get_attribute("data-index") for fileElem in fileElems];
    fileURLs = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, '//tr[@class="MuiTableRow-root css-1bun0x4"]//a')));
    fileURLs = [fileURL.get_attribute("href") for fileURL in fileURLs];

    for idx, index in enumerate(fileIndes):
        fileCollection[index] = fileURLs[idx];
    return fileCollection;

def downloadFile(index: str, url: str, saveDir: str) -> None:
    """
    **Description**  
    Download a single file from a given URL and save it locally.  
    The file will be named using its index and the original file name.

    **Params**  
    - `index`: the identifier key for the file  
    - `url`: the file download URL  
    - `saveDir`: the target directory to save files (default "./Downloads")

    **Returns**  
    - None
    """
    try:
        os.makedirs(saveDir, exist_ok=True);
        fileName = os.path.join(saveDir, f"{index}_{os.path.basename(url)}");
        start_time = time.perf_counter()
        headers = {"User-Agent": get_user_agent_of_pc()};
        with requests.get(url, stream=True, timeout=30, headers=headers) as response:
            response.raise_for_status()
            total_size = int(response.headers.get('Content-Length', 0))
            position = int(index) % 1000 if index.isdigit() else 0
            with open(fileName, "wb") as f, tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Downloading {fileName}", position=position) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk);
                        pbar.update(len(chunk))
        elapsed = time.perf_counter() - start_time
        print(f"Downloaded: {fileName} in {elapsed:.2f} seconds");
    except Exception as e:
        print(f"Failed to download {url} — error: {e}");

def multiThreadDownload(fileURLs: dict, maxThreads: int = 8, saveDir: str = "./Downloads") -> None:
    """
    **Description**  
    Use multithreading to download all files in parallel from the fileURLs dict.

    **Params**  
    - `fileURLs`: dictionary where key = index, value = file URL  
    - `maxThreads`: number of threads to run in parallel (default 8)

    **Returns**  
    - None
    """
    with ThreadPoolExecutor(max_workers=maxThreads) as executor:
        futures = [
            executor.submit(downloadFile, index, url, saveDir)
            for index, url in fileURLs.items()
        ];
        for future in as_completed(futures):
            future.result();
    
def main(driver: webdriver.Edge) -> None:
    """
    **Description**  
    Launches the target URL, scrolls through the table container to trigger all lazy-loaded rows,  
    collects all downloadable file links, and initiates multithreaded downloading.

    **Params**  
    - `driver`: the Selenium Edge driver instance

    **Returns**  
    - None
    """
    fileCollection = {};
    driver.get(URL);
    
    
    table = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//div[@class="MuiTableContainer-root css-1p6ntod"]')));
    scrollTopLast = -1;
    while True:
        getfileURLs(driver, fileCollection);
        
        # Scroll
        driver.execute_script("arguments[0].scrollTop += 1500;", table);
        time.sleep(0.01);
        
        # If the scroll stops
        scrollTopNow = driver.execute_script("return arguments[0].scrollTop;", table);
        if scrollTopNow == scrollTopLast:
            break;
        scrollTopLast = scrollTopNow;
    
    
    
    saveDir = "/storage/yangjianLab/wanfang/hprc_FASTA_latest";
    # 1. Delete files smaller than 730MB
    fileToDownload = [];
    if os.path.exists(saveDir):
        for filename in os.listdir(saveDir):
            if filename.endswith(".fa.gz"):
                filepath = os.path.join(saveDir, filename);
                size_bytes = os.path.getsize(filepath);
                if size_bytes <= 730 * 1024 * 1024:
                    prefix = filename.split("_")[0];
                    fileToDownload.append(prefix);
                    os.remove(filepath);
                    print(f"Deleted small file: {filename}");
    # 2. Fetch existing prefixes
    exist_prefixes = [];
    for filename in os.listdir(saveDir):
        if filename.endswith(".fa.gz"):
            prefix = filename.split("_")[0];
            try:
                exist_prefixes.append(int(prefix));
            except ValueError:
                continue;
    if exist_prefixes:
        max_prefix = max(exist_prefixes);
        # 3. Delete max_prefix-10 to max_prefix
        for num in range(max_prefix - 10, max_prefix + 1):
            prefix_str = str(num);
            for fname in os.listdir(saveDir):
                if fname.startswith(prefix_str + "_") and fname.endswith(".fa.gz"):
                    fileToDownload.append(prefix_str);
                    os.remove(os.path.join(saveDir, fname));
                    print(f"Deleted file by prefix in range: {fname}");
        # 4. Ddd non-reached prefixes to fileToDownload
        for num in range(max_prefix, 560):
            fileToDownload.append(str(num));
    # 5. build fileToDownload
    download_dict = {idx: fileCollection[idx] for idx in fileToDownload if idx in fileCollection};
    print(f"{len(fileToDownload)} of {len(fileCollection)} files to download");
    multiThreadDownload(download_dict, maxThreads=8, saveDir=saveDir);
    driver.quit();
    
    
# Entry point of the script: configure Edge options and start crawling
if __name__ == "__main__":

    options = Options();
    options.binary_location = "/home/yangjianLab/wanfang/chrome-linux/chrome";
    options.add_argument("--headless");
    options.add_argument("--window-size=1920,1080");
    options.add_argument(f"user-agent={get_user_agent_of_pc()}");
    options.add_argument("disable-infobas");
    options.add_argument("--diable-blink-features");
    options.add_experimental_option("excludeSwitches", ["enable-automation"]);
    options.add_argument("--diable-blink-features=AutomationControlled");

    service = Service("/home/yangjianLab/wanfang/chromedriver_linux64/chromedriver");
    driver = webdriver.Chrome(service=service, options=options);
    
    main(driver);