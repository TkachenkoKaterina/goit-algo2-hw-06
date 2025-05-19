import argparse
import string
import sys
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

import requests
import matplotlib.pyplot as plt


def get_text(url: str) -> str | None:
    """Завантажує текст з URL."""
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}", file=sys.stderr)
        return None


def remove_punctuation(text: str) -> str:
    """Видаляє пунктуацію з тексту."""
    return text.translate(str.maketrans('', '', string.punctuation))


def map_function(word: str) -> tuple[str, int]:
    """Map-функція: повертає пару (слово, 1)."""
    return word.lower(), 1


def shuffle_function(
    mapped: list[tuple[str, int]]
) -> list[tuple[str, list[int]]]:
    """
    Групує проміжні результати за ключем:
    [("a",1), ("b",1), ("a",1)] → [("a",[1,1]), ("b",[1])]
    """
    d: dict[str, list[int]] = defaultdict(list)
    for key, val in mapped:
        d[key].append(val)
    return list(d.items())


def reduce_function(item: tuple[str, list[int]]) -> tuple[str, int]:
    """Reduce-функція: підсумовує список значень для кожного ключа."""
    key, vals = item
    return key, sum(vals)


def map_reduce(text: str) -> dict[str, int]:

    cleaned = remove_punctuation(text)
    words = cleaned.split()

    # Map
    with ThreadPoolExecutor() as ex:
        mapped = list(ex.map(map_function, words))

    # Shuffle
    grouped = shuffle_function(mapped)

    # Reduce
    with ThreadPoolExecutor() as ex:
        reduced = list(ex.map(reduce_function, grouped))

    return dict(reduced)


def visualize_top_words(freq: dict[str, int], top_n: int = 10) -> None:

    top = sorted(freq.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
    words, counts = zip(*top)

    plt.figure(figsize=(10, 6))
    plt.barh(words[::-1], counts[::-1])
    plt.title(f"Top {top_n} Most Frequent Words")
    plt.xlabel("Frequency")
    plt.ylabel("Words")
    plt.tight_layout()
    plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="MapReduce word count + visualization"
    )
    parser.add_argument(
        '-n', '--top',
        type=int,
        default=10,
        help="How many top words to show and visualize (default: 10)"
    )
    args = parser.parse_args()

    # URL для завантаження тексту
    url = "https://gutenberg.net.au/ebooks01/0100021.txt"

    # Завантажуємо текст
    text = get_text(url)
    if text is None:
        sys.exit(1)

    # Виконуємо MapReduce
    freq = map_reduce(text)

    # Візуалізуємо топ-N слів
    visualize_top_words(freq, top_n=args.top)
