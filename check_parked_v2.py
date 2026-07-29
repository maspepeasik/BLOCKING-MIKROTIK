import asyncio
import aiohttp
import time

INPUT_FILE = "filtered_genuine.txt"
OUTPUT_GENUINE = "filtered_genuine_v2.txt"
OUTPUT_PARKED = "filtered_parked_v2.txt"
BATCH_SIZE = 2000
MAX_CONCURRENT = 1000
TIMEOUT = 5
MAX_READ = 2048

PARKING_DOMAINS = [
    "forsale.godaddy.com",
    "afternic.com",
    "hugedomains.com",
    "buydomains.com",
    "domainmarket.com",
    "sedo.com",
    "dan.com",
    "undeveloped.com",
    "brandbucket.com",
    "bodis.com",
    "above.com",
    "parking",
    "tdfs_",
    "bodis",
]

JS_REDIRECT_PATTERNS = [
    "window.location",
    "window.onload",
    "location.href",
]

semaphore = None

async def check_domain(session, domain):
    global semaphore
    async with semaphore:
        try:
            async with session.get(
                "https://" + domain,
                timeout=aiohttp.ClientTimeout(total=TIMEOUT),
                allow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            ) as resp:
                raw = await resp.content.read(MAX_READ)
                text = raw.decode("utf-8", errors="ignore").lower()

                is_js_redirect = any(p in text for p in JS_REDIRECT_PATTERNS) and len(text) < 300

                if is_js_redirect:
                    try:
                        async with session.get(
                            "https://" + domain + "/lander",
                            timeout=aiohttp.ClientTimeout(total=TIMEOUT),
                            allow_redirects=True,
                            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
                        ) as resp2:
                            final_url = str(resp2.url).lower()
                            for pd in PARKING_DOMAINS:
                                if pd in final_url:
                                    return domain, True
                    except:
                        pass

                return domain, False
        except:
            return domain, False

async def main():
    global semaphore
    total_domains = 0
    with open(INPUT_FILE) as f:
        for _ in f:
            total_domains += 1

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    total = 0
    genuine_count = 0
    parked_count = 0
    start = time.time()

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0)) as session:
        with (
            open(OUTPUT_GENUINE, "w", buffering=1024*1024) as f_genuine,
            open(OUTPUT_PARKED, "w", buffering=1024*1024) as f_parked
        ):
            with open(INPUT_FILE, "r") as f:
                batch = []
                for line in f:
                    domain = line.strip()
                    if not domain:
                        continue
                    batch.append(domain)

                    if len(batch) >= BATCH_SIZE:
                        tasks = [check_domain(session, d) for d in batch]
                        results = await asyncio.gather(*tasks)
                        for domain, is_parked in results:
                            total += 1
                            if is_parked:
                                parked_count += 1
                                f_parked.write(domain + "\n")
                            else:
                                genuine_count += 1
                                f_genuine.write(domain + "\n")

                        elapsed = time.time() - start
                        rate = total / elapsed if elapsed > 0 else 0
                        eta = (total_domains - total) / rate if rate > 0 else 0
                        print(f"  {total:,}/{total_domains:,} | G:{genuine_count:,} P:{parked_count:,} | {rate:.0f}/s | ETA: {eta/60:.0f}m", flush=True)
                        batch = []
                        await asyncio.sleep(0)

                if batch:
                    tasks = [check_domain(session, d) for d in batch]
                    results = await asyncio.gather(*tasks)
                    for domain, is_parked in results:
                        total += 1
                        if is_parked:
                            parked_count += 1
                            f_parked.write(domain + "\n")
                        else:
                            genuine_count += 1
                            f_genuine.write(domain + "\n")

    elapsed = time.time() - start
    rate = total / elapsed if elapsed > 0 else 0
    print(f"\nDone! {total:,} | Genuine: {genuine_count:,} | Parked: {parked_count:,}", flush=True)
    print(f"Time: {elapsed:.1f}s | Rate: {rate:.0f}/s", flush=True)
    print(f"{OUTPUT_GENUINE} ({genuine_count:,} domains)", flush=True)
    print(f"{OUTPUT_PARKED} ({parked_count:,} domains)", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
