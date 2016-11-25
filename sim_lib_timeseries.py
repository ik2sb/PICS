import math 
import numpy as np
import statsmodels.api as sm
import pandas as pd
import sim_lib_common as clib

# lib1: Weighted Moving Average
def WMA (x):
    weights = np.linspace(0, 1, len(x))
    weights_sum = np.sum(weights)
    return np.dot(x, weights)/float(weights_sum)


# Exponentially Weighted Moving Average
def EWMA (x, alpha):

    assert 0 <= alpha <= 1

    N = x.size
    s = np.zeros(( N, ))

    s[0] = x[0]
    for i in range (1, N):

        s[i] = alpha * x[i] + ( 1 - alpha )*( s[i-1])

    return s[-1]

def Holt_Winters_Double_EWMA(x, alpha, beta):

    assert 0 <= alpha <= 1
    assert 0 <= beta <= 1

    N = x.size

    s = np.zeros(( N, ))
    b = np.zeros(( N, ))

    s[0] = x[0]
    for i in range( 1, N ):

        s[i] = alpha * x[i] + ( 1 - alpha )*( s[i-1] + b[i-1] )
        b[i] = beta * ( s[i] - s[i-1] ) + ( 1 - beta ) * b[i-1]

    #res = alpha * x[i+1] + ( 1 - alpha )*( s[i] + b[i] )

    pred = s[-1] + b[-1]
    return pred

def Brown_Double_EWMA(x, alpha):

    N = x.size

    s1 = np.zeros(( N, ))
    s2 = np.zeros(( N, ))

    s1[0] = x[0]
    s2[0] = x[0]

    for i in range( 1, N ):

        s1[i] = alpha * x[i]  + (1 - alpha)*s1[i-1]
        s2[i] = alpha * s1[i] + (1 - alpha)*s2[i-1]

    a_t = 2 * s1[-1] - s2[-1]
    b_t = alpha * (s1[-1] - s2[-1]) / (1 - alpha)
    m = 1

    pred = a_t + m * b_t
    return pred

# Autoregressive
def AR (x, p):

    pred_ptr = x.size

    try:
        ar_model = sm.tsa.AR(x, freq='A')
        ar_res = ar_model.fit(maxlag = p, method = 'mle', disp = -1)
    except:
        return float('nan')

    pred_results = ar_res.predict(pred_ptr-1, pred_ptr)
    return pred_results[-1]

# Autoregressive and Moving Average
def ARMA (x, p, q):

    pred = x.size

    try:
        arma_mod = sm.tsa.ARMA (x, order=(p, q))
        arma_res = arma_mod.fit(trend='nc', disp=-1)
    except:
        return float('nan')

    result, _, _ = arma_res.forecast(1)
    return result

# Autoregressive Integrated Moving Average
def ARIMA (x, p, d, q):
    pred = x.size
    try:
        arima_mod = sm.tsa.ARIMA (x, order=(p, d, q))
        arima_res = arima_mod.fit(trend='nc', disp=-1)
    except:
        return float('nan')

    result, stderr, conf_int = arima_res.forecast(1)
    return result
