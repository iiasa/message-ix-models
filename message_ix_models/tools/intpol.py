def intpol(y1, y2, x1, x2, x, dataframe=False):
    """Interpolate between (*x1*, *y1*) and (*x2*, *y2*) at *x*.

    Parameters
    ----------
    y1, y2 : float or pd.Series
    x1, x2, x : int
    dataframe : boolean (default=True)
        Option to consider checks appropriate for dataframes/series or not.
    """
    if dataframe is False and x2 == x1 and y2 != y1:
        print(">>> Warning <<<: No difference between x1 and x2," "returned empty!!!")
        return []
    elif dataframe is False and x2 == x1 and y2 == y1:
        return y1
    else:
        if x2 == x1 and dataframe is True:
            return y1
        else:
            y = y1 + ((y2 - y1) / (x2 - x1)) * (x - x1)
            return y