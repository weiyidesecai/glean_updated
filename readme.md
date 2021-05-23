## Install Dependencies

Environment: Ubuntu 20.04

```bash
sudo apt-get install default-jre-headless
pip install -r requirements.txt
```

## Prepair Data

Unpack `.csv` files into `./data` folder.

## Run

```bash
python3 invoice.py
```

The output will resides in `./data/gleans`.

To run unit tests:
```bash
python3 -m unittest test
```

## Run in Docker

```bash
docker build . -t invoice
docker run --rm -v "$(pwd)/data:/app/data" invoice
```
