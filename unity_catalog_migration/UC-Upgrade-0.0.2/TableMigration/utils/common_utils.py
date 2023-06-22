def get_value(lst: list, idx: int, idy: int, default: string) -> :
    try:
        return lst[idx][idy]
    except IndexError:
        return default