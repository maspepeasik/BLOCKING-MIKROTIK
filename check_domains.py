import asyncio
import aiodns
import socket
import time

INPUT_FILE = "SITUS-JUDI-SLOT-ONLINE.txt"
OUTPUT_ACTIVE = "filtered_active.txt"
OUTPUT_INACTIVE = "filtered_inactive.txt"
BATCH_SIZE = 20000
MAX_CONCURRENT = 2000
REPORT_INTERVAL = 100000

async def main():
    resolver = aiodns.DNSResolver(nameservers=["8.8.8.8", "1.1.1.1"])
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    total = 0
    active_count = 0
    inactive_count = 0
    start = time.time()

    async def check_domain(domain):
        domain = domain.strip()
        if not domain:
            return None
        async with semaphore:
            try:
                await resolver.getaddrinfo(domain, socket.AF_INET)
                return domain, True
            except (OSError, aiodns.error.DNSError):
                return domain, False

    with (
        open(OUTPUT_ACTIVE, "w", buffering=1024*1024) as f_active,
        open(OUTPUT_INACTIVE, "w", buffering=1024*1024) as f_inactive
    ):
        with open(INPUT_FILE, "r") as f:
            batch = []

            for line in f:
                batch.append(line)

                if len(batch) >= BATCH_SIZE:
                    tasks = [check_domain(d) for d in batch]
                    results = await asyncio.gather(*tasks)

                    for result in results:
                        if result is None:
                            continue
                        domain, is_active = result
                        total += 1
                        if is_active:
                            active_count += 1
                            f_active.write(domain + "\n")
                        else:
                            inactive_count += 1
                            f_inactive.write(domain + "\n")

                    if total % REPORT_INTERVAL == 0:
                        elapsed = time.time() - start
                        rate = total / elapsed if elapsed > 0 else 0
                        eta = (1940000 - total) / rate if rate > 0 else 0
                        print(f"  {total:,} | A:{active_count:,} I:{inactive_count:,} | {rate:.0f}/s | {elapsed:.1f}s | ETA:{eta/60:.0f}m")

                    batch = []
                    await asyncio.sleep(0)

            if batch:
                tasks = [check_domain(d) for d in batch]
                results = await asyncio.gather(*tasks)

                for result in results:
                    if result is None:
                        continue
                    domain, is_active = result
                    total += 1
                    if is_active:
                        active_count += 1
                        f_active.write(domain + "\n")
                    else:
                        inactive_count += 1
                        f_inactive.write(domain + "\n")

    elapsed = time.time() - start
    rate = total / elapsed if elapsed > 0 else 0
    print(f"\nDone! {total:,} | Active: {active_count:,} | Inactive: {inactive_count:,}")
    print(f"Time: {elapsed:.1f}s | Rate: {rate:.0f}/s")
    print(f"{OUTPUT_ACTIVE} ({active_count:,} domains)")
    print(f"{OUTPUT_INACTIVE} ({inactive_count:,} domains)")

if __name__ == "__main__":
    asyncio.run(main())
