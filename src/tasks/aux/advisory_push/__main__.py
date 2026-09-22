"""Process advisory repo changes: send Kafka messages, update Pyxis."""

from .advisory_push import main

if __name__ == "__main__":
    main()
