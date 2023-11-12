import os
import pandas as pd
from main import read_dataset, generate_summary_statistics, create_data_visualization

def test_read_dataset():
    data_csv = read_dataset('tests/test.csv')
    assert isinstance(data_csv, pd.DataFrame)
    assert not data_csv.empty

def test_generate_summary_statistics():
    data = read_dataset('tests/test.csv')
    
    summary = generate_summary_statistics(data)
    assert isinstance(summary, dict)
    assert "mean" in summary
    assert "median" in summary
    assert "std_dev" in summary

    try:
        generate_summary_statistics(pd.DataFrame())
    except ValueError as e:
        assert str(e) == "Data cannot be None or empty"

def test_create_data_visualization():
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    data = read_dataset('tests/test.csv')
    output_file = 'output/test_data_visualization.png'
    create_data_visualization(data, output_file)
    assert os.path.isfile(output_file)

    try:
        create_data_visualization(pd.DataFrame(), 'output/test_data_visualization_fail.png')
    except ValueError as e:
        assert str(e) == "Data cannot be None or empty"

    if os.path.isfile(output_file):
        os.remove(output_file)
    if os.path.isfile('output/test_data_visualization_fail.png'):
        os.remove('output/test_data_visualization_fail.png')

if __name__ == "__main__":
    test_read_dataset()
    test_generate_summary_statistics()
    test_create_data_visualization()
