try:
    from .pipeline import main
except ImportError:
    from pipeline import main


if __name__ == "__main__":
    main()
