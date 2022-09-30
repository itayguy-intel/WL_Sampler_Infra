"""
This script aligns ThermalPy and EMON trace that collected in the same run for a workload. The execution flow should be:
1. Start EMON (use -V option)
2. Start ThermalPy
3. Run DAQAlign.exe
4. Run workload
5. Run DAQAlign.exe
6. Stop ThermalPy
7. Stop EMON

Then take the result emon.csv and thermalpy.csv and run:
C:\\Program Files\\SPEED\\speed.exe run emon_thermalpy_align.py --emon-file emon.csv --thermalpy_file thermalpy.csv
    --output-file out.csv

The script will also generate health report as HTML file in the same location as the output file that shows the
alignment accuracy
"""
import argparse
import sys
import pandas
import numpy

from scipy.signal import find_peaks
from tracedm.emon import parse
from reports import *


def is_mean_column(column):
    if column.startswith('DTS') or 'ratio' in column or 'Frequency' in column:
        return True
    return False


def is_sum_column(column):
    if column == 'cycles':
        return True
    return False


def _parse_command_line(argv):
    parser = argparse.ArgumentParser(
        prog='emon_thermalpy_align.py',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument("--emon-file", "-e", help="Path to emon CSV data file (generated from EMON "
                                                  "with -V switch)", required=True)
    parser.add_argument("--thermalpy-file", "-t", help="Path to thermalpy trace file", required=True)
    parser.add_argument("--output-file", "-o", help="Path to output CSV file", required=True)

    # add your arguments here
    return parser.parse_args(args=argv)


def _load_traces(emon_file, thermalpy_file):
	# This code fixes the EMON
    with open(emon_file, 'r') as in_file:
        with open(r'C:\Temp\emon.txt', 'w') as out_file:
            next_line = False
            for line in in_file:
                line = line.strip()
                if next_line:
                    while line[-2] == ';':
                        line = line[:-1]

                out_file.write(line + '\n')
                if line.startswith('epoch'):
                    next_line = True
                else:
                    next_line = False

    emon_file = r'C:\Temp\emon.txt'
    emon_trace = parse(emon_file)

    thermalpy_trace = pandas.read_csv(thermalpy_file)
    thermalpy_trace.set_index('Frame', inplace=True)
    thermalpy_trace.reset_index(inplace=True)
	# Convert the Time column from MS to SEC
    # thermalpy_trace['Time'] /= 1000
    thermalpy_trace.set_index('Time', inplace=True)
    return emon_trace, thermalpy_trace


def step_resample(series: pandas.Series, period: float, mode='after') -> pandas.Series:
    """
    | Resample step function specified by "series" with specified period.
    | The step function is assumed to have steps AFTER the index points (example: [(0, 1), (1, 2), ...] means [f=1 @ 0]
    -> [f=2 @ 1] ...)

    :param series: pandas.Series representing a step function
    :param period: resample period
    :param mode: step mode. One of 'after' or 'before' (default: 'after')
    :return: pandas.Series representing a step function with duration-weighted average per step (step AFTER point)

    :example:

        >>> # resample specified step function with constant period of 1.0:
        >>> res = step_resample(pandas.Series([1, 2, 1, 0], index=[1.1, 1.2, 3.2, 3.3]), period=1.0)
            1.1 2.9,
            2.1 2.0,
            3.1 0.3
    """
    if mode != 'after':
        raise NotImplementedError()

    # implemented using interval intersection between grid and step intervals
    left = series.index[0]
    right = left + period

    result_x = [left]
    result_y = [0]

    for step_left, step_right, value in zip(series.index.values[:-1], series.index.values[1:], series.values[:-1]):
        while True:
            # interval intersection:
            ileft, iright = max(left, step_left), min(step_right, right)
            if ileft < iright:
                result_y[-1] = result_y[-1] + (iright - ileft) * value

            if step_right >= right:
                # shift the grid, keep the step interval
                left = right
                right = left + period

                result_x.append(left)
                result_y.append(0)
            else:
                break  # shift interval

    result = pandas.Series(result_y, index=result_x)
    result /= period
    return result


def normalize(array):
    """
    Normalize the array into 0..1 range
    """
    result = array - array.min()
    return result / result.max()


def find_pattern(signal, pattern):
    """
    Use correlation to find "pattern" in "values", assuming uniform sampling.
    Will return location with the highest correlation between the pattern and the signal samples.

    :param signal: sequence of values representing a signal series
    :param pattern: sequence of values representing a signal sample to find
    :return: non-zero offset of "pattern" inside "signal"

    :example:

        >>> find_pattern([0, 0, 0, 2.0, 2.1, 0, 0, 0], [0, 1.3, 1.3, 0, 0])  # find approximate pattern match for 1D signal
        >>> 2

    """
    signal = numpy.atleast_1d(signal)
    signal = signal - signal.mean()

    pattern = numpy.atleast_1d(pattern)
    pattern = pattern - pattern.mean()

    return numpy.argmax(numpy.correlate(signal, pattern, mode='full')) - pattern.shape[0] + 1


def align(emon_file, thermalpy_file, output_file):
    """
    Aligns EMON csv file and thermalpy trace. The DAQAlign.exe software should be executed before and after the
    workload. It generates the output to the given output file

    :param emon_file: Path to EMON CSV file generated from EMON with -V switch
    :param thermalpy_file: path to file generated using thermalpy tool
    """
    SIZE_HINT = 'wide'

    report = Report('EMON-Thermalpy alignment health')
    emon_trace, thermalpy_trace = _load_traces(emon_file=emon_file, thermalpy_file=thermalpy_file)

    for core_type in ['bigcore', 'core', 'atom']:
        try:
            emon_trace.data['Duration'] = emon_trace.data[
                                              ('package0', core_type, 'CPU0', 'CPU_CLK_UNHALTED.REF_TSC')] / (
                                                  2995.20 * 1000000)
            emon_trace.data['Frequency0'] = emon_trace.data[
                                                ('package0', core_type, 'CPU0', 'CPU_CLK_UNHALTED.THREAD')] / \
                                            emon_trace.data['Duration'] / 1e6
        except:
            pass

    thermalpy_freq_data = thermalpy_trace['Frequency[MHz]'].copy()
    emon_freq_data = emon_trace.data['Frequency0'].copy()
    initial_state_section = Section('Initial',
                                    ChartGroup(
                                        ScatterChart('EMON CPU0 frequency', ScatterDataSeries(
                                            x=emon_freq_data.index, y=emon_freq_data, step=True,
                                            color='black'), sizehint=SIZE_HINT, markers=False),
                                        ScatterChart('ThermalPy CPU0 frequency', ScatterDataSeries(
                                            x=thermalpy_freq_data.index,
                                            y=thermalpy_freq_data, step=True, color='blue'),
                                                     sizehint=SIZE_HINT),
                                    ))
    report.append(initial_state_section)

    duration_diff = emon_trace.data.index[-1] - thermalpy_trace.index[-1]
    emon_df = emon_trace.data
    emon_df = emon_df.loc[[x for x in emon_df.index if x >= duration_diff]]

    emon_df = emon_df[emon_df.columns[2:]]
    emon_df.index -= emon_df.index[0]

    thermalpy_freq_data = thermalpy_trace['Frequency[MHz]'].copy()
    sampling_period = numpy.diff(thermalpy_freq_data.index).mean()
    pattern = normalize(
        step_resample(
            emon_df['Frequency0'],
            sampling_period
        ).values
    )
    offset = find_pattern(signal=thermalpy_freq_data, pattern=pattern)
    emon_df.index += thermalpy_freq_data.index[offset]

    alignment_state_section = Section('Alignment',
                                      ChartGroup(
                                          ScatterChart('EMON CPU0 frequency', ScatterDataSeries(
                                              x=emon_df['Frequency0'].index, y=emon_df['Frequency0'], step=True,
                                              color='black'), sizehint=SIZE_HINT),
                                          ScatterChart('ThermalPy CPU0 frequency', ScatterDataSeries(
                                              x=thermalpy_trace['Frequency[MHz]'].index,
                                              y=thermalpy_trace['Frequency[MHz]'], step=True, color='blue'),
                                                       sizehint=SIZE_HINT),
                                      ))
    report.append(alignment_state_section)

    # Combine the traces
    emon_df.index = numpy.round(emon_df.index, 6)
    thermalpy_trace.index = numpy.round(thermalpy_trace.index, 6)

    emon_ranges = list(zip(emon_df.index[:-1], emon_df.index[1:]))
    new_thermalpy_index_col = []
    emon_range_index = 0
    thermalpy_row_index = 0
    while thermalpy_row_index < thermalpy_trace.shape[0]:
        left, right = emon_ranges[emon_range_index]
        value = thermalpy_trace.index.values[thermalpy_row_index]
        if value <= left:
            new_thermalpy_index_col.append(left)
            thermalpy_row_index += 1
            continue

        if value <= right:
            new_thermalpy_index_col.append(right)
            thermalpy_row_index += 1
            continue

        emon_range_index += 1

    thermalpy_trace.index = new_thermalpy_index_col
    thermalpy_trace.index.name = 'Time'
    mean_columns = [c for c in thermalpy_trace.columns if is_mean_column(c)]
    grouped_thermalpy_trace_mean = thermalpy_trace[mean_columns].groupby('Time').mean().copy()
    grouped_thermalpy_trace_mean = grouped_thermalpy_trace_mean.iloc[1:]

    sum_columns = [c for c in thermalpy_trace.columns if is_sum_column(c)]
    grouped_thermalpy_trace_sum = thermalpy_trace[sum_columns].groupby('Time').sum().copy()
    grouped_thermalpy_trace_sum = grouped_thermalpy_trace_sum.iloc[1:]

    emon_df = emon_df.loc[[x for x in emon_df.index if grouped_thermalpy_trace_mean.index[
        0] <= x <= grouped_thermalpy_trace_mean.index[-1]]]
    emon_df = emon_df.drop([('Duration', '', '', '')], axis=1)
    combined_df = pandas.concat([emon_df, grouped_thermalpy_trace_mean, grouped_thermalpy_trace_sum], axis=1)
    new_cols = []
    for column in combined_df.columns:
        if isinstance(column, tuple):
            new_cols.append('-'.join(column))
        else:
            new_cols.append(column)
    combined_df.columns = new_cols
    combined_df = combined_df[combined_df['Frequency[MHz]'].notnull()]
    combined_df.columns = [c.replace('---', '') for c in combined_df.columns]

    combined_state_section = Section('Combined',
                                     ChartGroup(
                                         ScatterChart('EMON CPU0 frequency', ScatterDataSeries(
                                             x=combined_df['Frequency0'].index, y=combined_df['Frequency0'],
                                             step=True, color='black'), sizehint=SIZE_HINT),
                                         ScatterChart('ThermalPy CPU0 frequency', ScatterDataSeries(
                                             x=combined_df['Frequency[MHz]'].index,
                                             y=combined_df['Frequency[MHz]'], step=True, color='blue'),
                                                      sizehint=SIZE_HINT),
                                     ))
    report.append(combined_state_section)

    width = int(0.9 / combined_df.index.to_frame().diff().mean())
    series = combined_df['Frequency0']
    peaks = find_peaks(series.values, height=0.95 * series.max(), width=width)
    peaks = list(peaks[0])
    left_peak_ts = series.index[peaks[0]] + 14
    right_peak_ts = series.index[peaks[-1]] - 14

    chopped_state_section = Section('Chopped',
                                    ChartGroup(
                                        ScatterChart('EMON CPU0 frequency',
                                                     ScatterDataSeries(
                                                         x=combined_df['Frequency0'].index, y=combined_df['Frequency0'],
                                                         step=True, color='black'),
                                                     ScatterDataSeries(
                                                         x=[left_peak_ts, left_peak_ts], y=[0, series.max()],
                                                         color='red'),
                                                     ScatterDataSeries(
                                                         x=[right_peak_ts, right_peak_ts], y=[0, series.max()],
                                                         color='red'), sizehint=SIZE_HINT
                                                     ),
                                        ScatterChart('ThermalPy CPU0 frequency', ScatterDataSeries(
                                            x=combined_df['Frequency[MHz]'].index,
                                            y=combined_df['Frequency[MHz]'], step=True, color='blue'),
                                                     sizehint=SIZE_HINT),
                                    ))
    report.append(chopped_state_section)

    combined_df = combined_df.loc[[x for x in combined_df.index if left_peak_ts <= x <= right_peak_ts]]
    combined_df.index.name = 'Time[sec]'
    combined_df.index -= combined_df.index[0]
    final_state_section = Section('Final',
                                  ChartGroup(
                                      ScatterChart('EMON CPU0 frequency', ScatterDataSeries(
                                          x=combined_df['Frequency0'].index, y=combined_df['Frequency0'],
                                          step=True, color='black'), sizehint=SIZE_HINT),
                                      ScatterChart('ThermalPy CPU0 frequency', ScatterDataSeries(
                                          x=combined_df['Frequency[MHz]'].index,
                                          y=combined_df['Frequency[MHz]'], step=True, color='blue'),
                                                   sizehint=SIZE_HINT),
                                  ))
    report.append(final_state_section)

    if not output_file.endswith('.csv'):
        output_file += '.csv'

    combined_df.to_csv(output_file)
    print(f'Generated combined trace: {output_file}')

    health_report_file = output_file[:-4] + '.html'
    render_report(report=report, html_file=health_report_file)
    print(f'Generated health report: {health_report_file}')


def main(argv):
    args = _parse_command_line(argv=argv)

    align(emon_file=args.emon_file, thermalpy_file=args.thermalpy_file, output_file=args.output_file)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
