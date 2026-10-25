"""Check private bucket access; optionally round-trip one temporary synthetic object."""

import argparse
import hashlib
from uuid import uuid4

from firewatch.config import Settings
from firewatch.evidence.storage import SupabaseStore


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--round-trip", action="store_true")
    args = parser.parse_args()
    result = False
    try:
        settings = Settings.model_validate(
            Settings().model_dump() | {"supabase_storage_enabled": True}
        )
        with SupabaseStore(settings) as store:
            if not store.check_bucket():
                print("Private bucket check failed; verify the URL, secret key and private bucket.")
                return 1
            if args.round_trip:
                content = ("FireWatch synthetic storage check " + uuid4().hex).encode()
                checksum = hashlib.sha256(content).hexdigest()
                try:
                    store.put(checksum, content)
                    result = store.get(checksum) == content
                finally:
                    # Delete only this run's unique synthetic object, never application evidence.
                    response = store.client.delete(store._path(checksum, download=False))
                    if response.status_code not in {200, 204}:
                        result = False
            else:
                result = True
    except Exception:
        # Never print provider bodies, settings, HTTP exceptions or credential-bearing URLs.
        result = False
    print("Private storage check passed." if result else "Private storage check failed.")
    return int(not result)


if __name__ == "__main__":
    raise SystemExit(main())
