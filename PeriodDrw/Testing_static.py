#!/usr/bin/env python3
"""
Neven Caplar
Last updated: 2023-12-07

Goals:
Understand statis input to jax

"""
import numpy as np
import time

import JaxPeriodDrwFit
import JaxPeriodDrwFit_nostatic
import jax

# import jax.numpy as jnp
# from jax import jit

jax.config.update("jax_log_compiles", False)

t = np.sort(
    np.array([58956.25185, 58955.30475, 58955.30152, 58944.25345, 58941.27305,
              58968.27727, 59290.30288, 58994.22464, 58995.20446])
)

y = np.array([19.312641, 19.383255, 19.29778, 19.140474, 19.288923,
              19.351116, 19.249687, 19.46319, 19.3588])

yerr = np.array([0.07915251, 0.0834377, 0.07827622, 0.0695373, 0.07775825,
                 0.08146235, 0.07550106, 0.08853268, 0.08193084])


if __name__ == "__main__":
    theta = [0.5, -1.0, 0.8, -2.0]
    JaxPeriodDrwFit_instance = JaxPeriodDrwFit.JaxPeriodDrwFit()
    t0 = time.time()
    res1 = JaxPeriodDrwFit_instance.neg_log_likelihood(np.array(theta), t, y, yerr)
    print("Second iteration starting here")
    t1 = time.time()
    res2 = JaxPeriodDrwFit_instance.neg_log_likelihood(2 * np.array(theta), t, y, yerr)
    t2 = time.time()
    print("First iteration time:", round(t1 - t0, 4), "seconds")
    # Report the time for the second iteration
    print("Second iteration time:", round(t2 - t1, 4), "seconds")

    JaxPeriodDrwFit_nostatic_instance = JaxPeriodDrwFit_nostatic.JaxPeriodDrwFit()
    t0s = time.time()
    res1_nostatic = JaxPeriodDrwFit_nostatic_instance.neg_log_likelihood(
        np.array(theta), t, y, yerr
    )
    t1s = time.time()
    res2_nostatic = JaxPeriodDrwFit_nostatic_instance.neg_log_likelihood(
        2 * np.array(theta), t, y, yerr
    )
    t2s = time.time()
    print("First iteration time:", round(t1s - t0s, 4), "seconds")
    # Report the time for the second iteration
    print("Second iteration time:", round(t2s - t1s, 4), "seconds")
