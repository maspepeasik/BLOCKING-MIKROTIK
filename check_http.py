import asyncio
import aiohttp
import time

INPUT_FILE = "filtered_active.txt"
OUTPUT_GENUINE = "filtered_genuine.txt"
OUTPUT_PARKED = "filtered_parked.txt"
BATCH_SIZE = 2000
MAX_CONCURRENT = 1000
TIMEOUT = 5
MAX_READ = 2048
REPORT_INTERVAL = 100000

PARKING_KEYWORDS = [
    "parked domain",
    "domain is parked",
    "domain for sale",
    "buy this domain",
    "this domain is for sale",
    "domain may be for sale",
    "purchase this domain",
    "domain is parked with",
    "domain parking",
    "parked free",
    "buy now. powered by",
    "sedo",
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
                text_lower = raw.decode("utf-8", errors="ignore").lower()
                for kw in PARKING_KEYWORDS:
                    if kw in text_lower:
                        return domain, True
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

                        if total % REPORT_INTERVAL == 0 or total % BATCH_SIZE == 0:
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
