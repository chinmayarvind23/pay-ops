"""Fixed bad-image entrypoint for the local startup regression scenario."""


def main() -> None:
    """Exit deterministically before opening any service port or making network calls."""
    raise RuntimeError("synthetic startup regression")


if __name__ == "__main__":
    main()
