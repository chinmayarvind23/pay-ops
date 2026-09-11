"""Local entry points default to loopback until authenticated deployment exists."""

import argparse

import uvicorn


def main() -> None:
    """Keep the first runnable command explicit about its mock-only scope."""
    parser = argparse.ArgumentParser(description="PayOps local incident API")
    parser.add_argument("command", choices=["serve"])
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    uvicorn.run("payops.api:create_app", factory=True, host="127.0.0.1", port=args.port)


if __name__ == "__main__":
    main()
