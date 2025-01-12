import pandas as pd


def scale_rd_to_touch[T: int | pd.Series](x: T, y: T) -> tuple[T, T]:
    """
    Scale RD coordinates to a (-1, 1) grid for use in TouchDesigner.

    RD range is 0 < x 280 and 300 < y < 625 (km)
    Center Amersfoort (155, 463) to (0, 0)
    """
    return (  # type: ignore
        (x - 155_000) / (325_000 / 2),
        (y - 463_000) / (325_000 / 2),
    )
