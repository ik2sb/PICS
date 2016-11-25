import sim_lib_common as clib
from scipy import stats
import numpy as np
import math

def build_regression_equation (coefs):

    eq = "Eq(X) = "
    maxnum = len(coefs)
    j = maxnum
    for x in xrange (maxnum):
        j -= 1

        if x != 0:
            eq += " + "

        eq += str(round(coefs[x],2))
        if j > 1:
            eq += "*X^"+str(j)
        elif j == 1:
            eq += "*X"

    return eq

#X, Y should be list
def regression (degree, x, y, value):

    if degree == 1:
        return linear_regression (x, y, value)

    elif degree == 2:
        return quadratic_regression (x, y, value)

    elif degree == 3:
        return cubic_regression (x, y, value)

    else:
        print "[Regress_] Regression Lib does not support %d degree regression" % degree
        clib.sim_exit()

def linear_regression (x, y, value):
    slope, intercept, r_value, p_value, std_err = stats.linregress(x,y)

    result  = int(math.ceil(slope * value + intercept))
    coefs   = [slope, intercept]

    return result, coefs

def quadratic_regression (x, y, value):
    x1 = np.array(x)
    y1 = np.array(y)

    coefs   = np.polyfit(x1, y1, 2)
    result  = int(math.ceil(coefs[0] * value**2 + coefs[1] * value + coefs[2]))

    return result, coefs

def cubic_regression (x, y, value):
    x1 = np.array(x)
    y1 = np.array(y)

    coefs   = np.polyfit(x1,y1,3)
    result  = int(math.ceil(coefs[0] * value ** 3 + coefs[1] * value ** 2 + coefs[2] * value + coefs[3]))

    return result, coefs
