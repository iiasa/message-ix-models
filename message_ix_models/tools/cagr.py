def CAGR(first, last, periods):
    """Calculate Annual Growth Rate

    Parameters
    ----------
    first : number
        value of the first period
    second : number
        value of the second period
    periods : number
        period length between first and second value

    Returns
    -------
    val : number
        calculated annual growth rate
    """

    val = (last / first) ** (1 / periods)
    val = val.rename(last.name)
    return val