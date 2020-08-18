# See https://stackoverflow.com/a/19521297/3187068
import matplotlib
matplotlib.use('pdf')
font = {'size': 14}
matplotlib.rc('font', **font)

from typing import List, NamedTuple, Tuple
import math
import matplotlib.pyplot as plt
import numpy as np

def main():
    fig, ax = plt.subplots(1, 1, figsize=(6.4, 4.8))

    for n in range(3, 10):
        fw = np.arange(0, 1, 1/1000)
        fr = 1 - fw
        load = (2 / n) * (fr**2 + (n-1)*fr*fw + fw**2)
        ax.plot(fw, load, label=str(n))

    ax.set_xlabel('Write fraction')
    ax.set_ylabel('Load')
    ax.grid()
    ax.legend(loc='best')
    output_filename = 'sharded_load.pdf'
    fig.savefig(output_filename, bbox_inches='tight')
    print(f'Wrote plot to {output_filename}.')


if __name__ == '__main__':
    main()
