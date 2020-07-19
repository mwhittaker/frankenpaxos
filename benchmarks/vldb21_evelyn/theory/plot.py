# See https://stackoverflow.com/a/19521297/3187068
import matplotlib
matplotlib.use('pdf')
font = {'size': 12}
matplotlib.rc('font', **font)

import argparse
import matplotlib.pyplot as plt
import os.path


def plot_throughput_vs_num_replicas(args) -> None:
    fig, ax = plt.subplots(1, 3, figsize=(3 * 6.4, 4.8))

    for fw in [0, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1]:
        fr = 1 - fw
        ns = list(range(2, 31))
        throughputs = [(n * args.alpha) / (n*fw + fr) for n in ns]
        write_throughputs = [fw * t for t in throughputs]
        read_throughputs = [fr * t for t in throughputs]
        ax[0].plot(ns, throughputs, '.-', label=f'{int(fw * 100)}% writes')
        ax[1].plot(ns, write_throughputs, '.-')
        ax[2].plot(ns, read_throughputs, '.-')

    for a in ax:
        a.set_xlabel('Number of replicas')
        a.grid()
    ax[0].set_ylabel('Peak throughput')
    ax[1].set_ylabel('Peak write throughput')
    ax[2].set_ylabel('Peak read throughput')
    ax[0].legend(loc='best')
    output_filename = os.path.join(args.output_dir, 'theory_tput_vs_replicas.pdf')
    fig.savefig(output_filename, bbox_inches='tight')
    print(f'Wrote plot to {output_filename}.')


def plot_throughput_vs_write_ratio(args) -> None:
    fig, ax = plt.subplots(1, 3, figsize=(3 * 6.4, 4.8))

    fws = [i / 100 for i in range(0, 100, 2)] + [1]
    frs = [1 - fw for fw in fws]
    for n in [1, 2, 3, 4, 5, 6, 7, 8, 9]:
        throughputs = [(n * args.alpha) / (n*fw + fr) for (fw, fr) in zip(fws, frs)]
        write_throughputs = [fw * t for (fw, t) in zip(fws, throughputs)]
        read_throughputs = [fr * t for (fr, t) in zip(frs, throughputs)]
        ax[0].plot(frs, throughputs, '.-', label=f'{n} replicas')
        ax[1].plot(frs, write_throughputs, '.-')
        ax[2].plot(frs, read_throughputs, '.-')

    fws = [i / 100 for i in range(10, 100, 2)] + [1]
    frs = [1 - fw for fw in fws]
    throughputs = [args.alpha / fw for fw in fws]
    write_throughputs = [fw * t for (fw, t) in zip(fws, throughputs)]
    read_throughputs = [fr * t for (fr, t) in zip(frs, throughputs)]
    ax[0].plot(frs, throughputs, '.-', label='infinite replicas')
    ax[1].plot(frs, write_throughputs, '.-')
    ax[2].plot(frs, read_throughputs, '.-')

    for a in ax:
        a.set_xlabel('Read fraction')
        a.grid()
    ax[0].set_ylabel('Peak throughput')
    ax[1].set_ylabel('Peak write throughput')
    ax[2].set_ylabel('Peak read throughput')
    ax[0].legend(loc='best')
    output_filename = os.path.join(args.output_dir, 'theory_tput_vs_fraction.pdf')
    fig.savefig(output_filename, bbox_inches='tight')
    print(f'Wrote plot to {output_filename}.')


def plot_nice_throughput_vs_num_replicas(args) -> None:
    fig, ax = plt.subplots(2, 1, figsize=(6.4, 2 * 4.8))

    for alpha_w in [f*args.alpha for f in [0, 0.1, 0.2, 0.3, 0.4, 0.5,
                                      0.75, 0.9, 0.95, 1]]:
        ns = list(range(2, 31))
        throughputs = [alpha_w + n*(args.alpha - alpha_w) for n in ns]
        write_throughputs = [alpha_w] * len(throughputs)
        read_throughputs = [t - alpha_w for t in throughputs]
        frs = [r / (r + w)
               for (r, w) in zip(read_throughputs, write_throughputs)]
        ax[0].plot(ns, throughputs, '.-', label=f'{int(alpha_w)} writes')
        ax[1].plot(ns, frs, '.-')

    for a in ax:
        a.set_xlabel('Number of replicas')
        a.grid()
    ax[0].set_ylabel('Peak throughput')
    ax[1].set_ylabel('Read fraction')
    ax[0].legend(loc='best')
    output_filename = os.path.join(args.output_dir,
                                   'theory_nice_tput_vs_replicas.pdf')
    fig.savefig(output_filename, bbox_inches='tight')
    print(f'Wrote plot to {output_filename}.')


def plot_nice_throughput_vs_writes(args) -> None:
    fig, ax = plt.subplots(1, 1, figsize=(6.4, 4.8))

    alpha_ws = [args.alpha * i / 50 for i in range(0, 51)]
    for n in [1, 2, 3, 4, 5, 6, 7, 8, 9]:
        throughputs = [w + n*(args.alpha - w) for w in alpha_ws]
        read_throughputs = [t - w for (t, w) in zip(throughputs, alpha_ws)]
        ax.plot(alpha_ws, throughputs, '.-', label=f'{n} replicas')

    ax.set_xlabel('Writes')
    ax.grid()
    ax.set_ylabel('Peak throughput')
    ax.legend(loc='best')
    output_filename = os.path.join(args.output_dir,
                                   'theory_nice_tput_vs_writes.pdf')
    fig.savefig(output_filename, bbox_inches='tight')
    print(f'Wrote plot to {output_filename}.')


def main(args) -> None:
    plot_throughput_vs_num_replicas(args)
    plot_throughput_vs_write_ratio(args)
    plot_nice_throughput_vs_num_replicas(args)
    plot_nice_throughput_vs_writes(args)


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--output_dir',
                        type=str,
                        default='.',
                        help='Output directory.')
    parser.add_argument('--alpha',
                        type=int,
                        default=100000,
                        help='Peak throughput of a single server.')
    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
