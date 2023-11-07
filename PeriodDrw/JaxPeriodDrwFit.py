import numpy as np
from functools import partial
import matplotlib.pyplot as plt
# import scipy.optimize as sco

import jax
import jax.numpy as jnp
from jax import jit
import jax.scipy.optimize as jsco

from tinygp import GaussianProcess
from tinygp.kernels import quasisep
jax.config.update("jax_enable_x64", True)


class JaxPeriodDrwFit():

    def __init__(self, t=None, y=None, yerr=None):
        # TODO: Every function has full and DRW option as everything is
        # defined twice; I do not know how to make this better at this point
        # TODO: custom specification of padded values
        # TODO: experiment with gpu
        self.y = y
        self.yerr = yerr
        self.jsoln_jax_ty_cpu = None

    def build_gp(self, theta, t, y, yerr):
        """Build a Gaussian Process model with drw + periodic kernel.

        Parameters
        ----------
        theta : array-like, (4,)
            Array of float values representing the parameters for the kernels.
        t : array-like
            Time domain data for the Gaussian Process.
        y : array-like
            Observations corresponding to the time domain data.
        yerr : array-like
            Uncertainties (errors) associated with the observations.

        Returns
        -------
        gp : GaussianProcess
            Gaussian Process model with the specified kernels.

        Example
        -------
        theta = [0.5, -1.0, 0.8, -2.0]
        gp = build_gp(theta, t, y, yerr)
        """

        assert len(theta) == 4

        log_drw_scale = theta[0]
        log_drw_amp = theta[1]
        log_per_scale = theta[2]
        log_per_amp = theta[3]
        exp_kernel = quasisep.Exp(
            scale=10**log_drw_scale, sigma=10**log_drw_amp)

        periodic_kernel = quasisep.Cosine(
            scale=10**(log_per_scale), sigma=10**(log_per_amp))

        kernel = exp_kernel + periodic_kernel
        self.kernel = kernel

        return GaussianProcess(kernel, t, diag=yerr, mean=np.mean(y))

    def build_gp_drw(self, theta, t, y, yerr):
        """Build a Gaussian Process model with the damped random walk kernel.

        Parameters
        ----------
        theta : array-like, (2,)
            Array of float values representing the parameters for the kernel.
        t : array-like
            Time domain data for the Gaussian Process.
        y : array-like
            Observations corresponding to the time domain data.
        yerr : array-like
            Uncertainties (errors) associated with the observations.

        Returns
        -------
        gp : GaussianProcess
            Gaussian Process model with the damped random walk kernel.

        Example
        -------
        theta = [0.5, -1.0, 0.8, -2.0]
        gp = build_gp(theta, t, y, yerr)
        """

        assert len(theta) == 2

        log_drw_scale = theta[0]
        log_drw_amp = theta[1]

        exp_kernel = quasisep.Exp(
            scale=10**log_drw_scale, sigma=10**log_drw_amp)

        kernel = exp_kernel
        self.kernel = kernel

        return GaussianProcess(kernel, t, diag=yerr, mean=np.mean(y))

    @partial(jit, static_argnums=(0,))
    def neg_log_likelihood(self, theta, t, y, yerr):
        """Compute the negative log-likelihood of a Gaussian Process model.

        Parameters
        ----------
        theta : array-like
            Array of float values representing the parameters for the kernels.
        t : array-like
            Time domain data for the Gaussian Process.
        y : array-like
            Observations corresponding to the time domain data.
        yerr : array-like
            Uncertainties (errors) associated with the observations.

        Returns
        -------
        neg_log_likelihood : float
            Negative log-likelihood of the Gaussian Process model.
        """

        gp = self.build_gp(theta, t, y, yerr)
        return -gp.log_probability(y)

    @partial(jit, static_argnums=(0,))
    def neg_log_likelihood_drw(self, theta, t, y, yerr):
        """Compute the negative log-likelihood of a DRW Gaussian Process model.

        Parameters
        ----------
        theta : array-like
            Array of float values representing the parameters for the kernels.
        t : array-like
            Time domain data for the Gaussian Process.
        y : array-like
            Observations corresponding to the time domain data.
        yerr : array-like
            Uncertainties (errors) associated with the observations.

        Returns
        -------
        neg_log_likelihood : float
            Negative log-likelihood of the Gaussian Process model.
        """

        gp = self.build_gp_drw(theta, t, y, yerr)
        return -gp.log_probability(y)

    def optimize(self, theta, t, y, yerr):
        """Optimize the parameters of a Gaussian Process model.

        Parameters
        ----------
        theta : array-like
            Array of float values representing the parameters for the kernels.
        t : array-like
            Time domain data for the Gaussian Process.
        y : array-like
            Observations corresponding to the time domain data.
        yerr : array-like
            Uncertainties (errors) associated with the observations.

        Returns
        -------
        jsoln.fun, jsoln.x : Jax array (1,), Jax array(4,)
            Optimized parameters for the Gaussian Process model.
        """

        jsoln = jsco.minimize(self.neg_log_likelihood, x0=jnp.array(theta),
                              method="bfgs",
                              args=(jnp.array(t),
                                    jnp.array(y),
                                    jnp.array(yerr)))

        return jsoln.fun, jsoln.x

    def optimize_drw(self, theta, t, y, yerr):
        """Optimize the parameters of a damped random walk Gaussian Process model.

        Parameters
        ----------
        theta : array-like
            Array of float values representing the parameters for the kernels.
        t : array-like
            Time domain data for the Gaussian Process.
        y : array-like
            Observations corresponding to the time domain data.
        yerr : array-like
            Uncertainties (errors) associated with the observations.

        Returns
        -------
        jsoln.fun, jsoln.x : Jax array (1,), Jax array(4,)
            Optimized parameters for the Gaussian Process model.
        """

        jsoln = jsco.minimize(self.neg_log_likelihood_drw, x0=jnp.array(theta),
                              method="bfgs",
                              args=(jnp.array(t),
                                    jnp.array(y),
                                    jnp.array(yerr)))

        return jsoln.fun, jsoln.x

    def optimize_map(self, t, y, yerr, n_init=100, use_pad=True):
        """Optimize the parameters of a Gaussian Process model using `map`.

        Parameters
        ----------
        n : int
            The number of times to create alternative theta initializations.
        t : array-like
            Time domain data for the Gaussian Process.
        y : array-like
            Observations corresponding to the time domain data.
        yerr : array-like
            Uncertainties (errors) associated with the observations.

        Returns
        -------
        res : array-like
            Array containing the results of the optimization process
            for different initial guesses.
        """

        sorted_indices = np.argsort(t)

        # Use these indices to sort y and yerr
        t = t[sorted_indices]
        y = y[sorted_indices]
        yerr = yerr[sorted_indices]

        if use_pad:
            n_pad = determine_pad(t)
        else:
            n_pad = 0

        # any0 large number to make the padded values irrelevant
        very_large_number = 900000000
        print(very_large_number)

        y_pad = jax.numpy.pad(y, (0, n_pad), mode='mean')
        t_pad = jax.numpy.pad(t, (0, n_pad), mode='constant', constant_values=very_large_number)
        yerr_pad = jax.numpy.pad(yerr, (0, n_pad), mode='constant', constant_values=very_large_number)

        t = jnp.array(t_pad)
        y = jnp.array(y_pad)
        yerr = jnp.array(yerr_pad)

        if self.jsoln_jax_ty_cpu is None:
            jsoln_jax_ty_cpu = jax.jit(self.optimize, backend="cpu")
            self.jsoln_jax_ty_cpu = jsoln_jax_ty_cpu
        else:
            pass

        # Create a partially applied function
        # with fixed values of t, y, and yerr
        partial_optimize = partial(self.jsoln_jax_ty_cpu, t=t, y=y, yerr=yerr)

        theta_init_matrix = \
            np.transpose(self.create_theta_init(n_init))
        soln_res_map = map(partial_optimize, theta_init_matrix)
        many_init_res = list(soln_res_map)
        # transforms jax outputs to single numpy array
        res = np.vstack(list(map(concatenate_arrays,
                                 jax.device_get(many_init_res))))
        self.res = res
        res_min = self.find_best_res(res)
        self.res_min = res_min

        return res_min

    def optimize_map_drw(self, t, y, yerr, n_init=100, use_pad=True):
        """Optimize the parameters of a Gaussian Process model using `map`.

        Parameters
        ----------
        n : int
            The number of times to create alternative theta initializations.
        t : array-like
            Time domain data for the Gaussian Process.
        y : array-like
            Observations corresponding to the time domain data.
        yerr : array-like
            Uncertainties (errors) associated with the observations.

        Returns
        -------
        res : array-like
            Array containing the results of the optimization process
            for different initial guesses.
        """

        sorted_indices = np.argsort(t)

        # Use these indices to sort y and yerr
        t = t[sorted_indices]
        y = y[sorted_indices]
        yerr = yerr[sorted_indices]
        """
        if use_pad:
            n_pad = determine_pad(t)
        else:
            n_pad = 0
        very_large_number = 1000000

        y_pad = jax.numpy.pad(y, (0, n_pad), mode='mean')
        t_pad = jax.numpy.pad(t, (0, n_pad), mode='constant', constant_values=very_large_number)
        yerr_pad = jax.numpy.pad(yerr, (0, n_pad), mode='constant', constant_values=very_large_number)

        t = jnp.array(t_pad)
        y = jnp.array(y_pad)
        yerr = jnp.array(yerr_pad)
        """ 
        if self.jsoln_jax_ty_cpu is None:
            jsoln_jax_ty_cpu = jax.jit(self.optimize_drw, backend="cpu")
            self.jsoln_jax_ty_cpu = jsoln_jax_ty_cpu
        else:
            pass

        # Create a partially applied function
        # with fixed values of t, y, and yerr
        partial_optimize = partial(self.jsoln_jax_ty_cpu, t=t, y=y, yerr=yerr)

        theta_init_matrix = \
            np.transpose(self.create_theta_init(n_init))
        # take only first two columns
        theta_init_matrix = theta_init_matrix[:, [0, 1]]
        soln_res_map = map(partial_optimize, theta_init_matrix)
        many_init_res = list(soln_res_map)
        # transforms jax outputs to single numpy array
        res = np.vstack(list(map(concatenate_arrays,
                                 jax.device_get(many_init_res))))
        self.res = res
        res_min = self.find_best_res(res)
        self.res_min = res_min

        return res_min

    def find_best_res(self, res):
        """Find the best result from the optimization results.

        Parameters
        ----------
        res : array-like
            Array containing the results of the optimization process.

        Returns
        -------
        best_res : array-like
            The result with the minimum negative log-likelihood value.

        Notes
        -------
        This function makes strong assumption that function likelihood-like
        result is specified in the first column of the result array
        """
        res_min = res[res[:, 0] == np.min(res[:, 0])][0]
        return res_min

    def create_theta_init(self, n=None):
        """Create alternative initializations for the optimization.

        Parameters
        ----------
        n : int or None, optional
            The number of alternative theta initializations to create.
            If `n` is not provided, the default value is set to 1.

        Returns
        -------
        alt_theta_init : ndarray
            Array containing alternative theta initializations.
        """
        if n is None:
            n = 1
        np.random.seed(42)
        theta_init = jnp.array([np.random.uniform(0, 5, n),
                               np.random.uniform(-3, 2, n),
                               np.random.uniform(0, 5, n),
                               np.random.uniform(-3, -0.25, n)])
        return theta_init

    def plot_res_1d(self, res):
        res_min = self.find_best_res(res)

        plt.figure(figsize=(16, 8))
        plt.subplot(221)
        plt.scatter(x=res[:, 1], y=res[:, 0])
        plt.scatter(x=res_min[1], y=res_min[0])
        plt.xlabel('log_drw_scale')
        plt.xlim(np.quantile(res[:, 1], [0.05, 0.95]))
        plt.ylim(res_min[0]-1, res_min[0]+10)

        plt.subplot(222)
        plt.scatter(x=res[:, 2], y=res[:, 0])
        plt.scatter(x=res_min[2], y=res_min[0])
        plt.xlabel('log_drw_amp')
        plt.xlim(np.quantile(res[:, 2], [0.05, 0.95]))
        plt.ylim(res_min[0]-1, res_min[0]+10)

        plt.subplot(223)
        plt.scatter(x=res[:, 3], y=res[:, 0])
        plt.scatter(x=res_min[3], y=res_min[0])
        plt.xlabel('log_per_scale')
        plt.xlim(np.quantile(res[:, 3], [0.05, 0.95]))
        plt.ylim(res_min[0]-1, res_min[0]+10)

        plt.subplot(224)
        plt.scatter(x=res[:, 4], y=res[:, 0])
        plt.scatter(x=res_min[4], y=res_min[0])
        plt.xlabel('log_per_amp')
        plt.xlim(np.quantile(res[:, 4], [0.05, 0.95]))
        plt.ylim(res_min[0]-1, res_min[0]+10)

    def plot_res_2d(self, res):
        res_min = self.find_best_res(res)
        plt.figure(figsize=(16, 8))
        plt.subplot(121)
        plt.scatter(x=res[:, 1], y=res[:, 2], c=res[:, 0])
        plt.axvline(res_min[1], ls=':')
        plt.axhline(res_min[2], ls=':')
        plt.xlabel('log_drw_scale')
        plt.ylabel('log_drw_amp')
        plt.ylim(-3, 0)
        plt.xlim(0, 4)

        plt.subplot(122)
        plt.scatter(x=res[:, 3], y=res[:, 4], c=res[:, 0])
        plt.axvline(res_min[3], ls=':')
        plt.axhline(res_min[4], ls=':')
        plt.xlabel('log_per_scale')
        plt.ylabel('log_per_amp')
        plt.ylim(-3, 0)
        plt.xlim(0, 4)
        plt.colorbar()


def concatenate_arrays(array_tuple):
    # Convert zero-dimensional array to a one-dimensional array
    array1 = array_tuple[0].flatten()
    # Concatenate the arrays
    return np.concatenate((array1, array_tuple[1]))


def determine_pad(t):
    """Determines by how many entries to pad the input arrays.

    Parameters
    ----------
    t : array-like
        Array containing the input. Only used for length calculation.

    Returns
    -------
    n_pad : int
        By how much the input has to be padded
    """
    # comp_sizes = np.array([10, 20, 40, 60, 80, 100, 120, 140, 160, 180, 200,
    #                       250, 300, 350, 400, 450, 500, 600, 700, 800, 900,
    #                       1000, 1200, 1400, 1600, 1800, 2000, 2500, 3000,
    #                       3500, 4000, 4500, 5000, 6000, 7000, 8000, 9000])
    
    comp_sizes = np.array([100, 200, 500, 2000, 5000, 9000])
    
    n_pad = comp_sizes[np.where(comp_sizes > len(t))][0] - len(t)
    return n_pad
