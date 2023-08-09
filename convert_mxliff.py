import xmltodict
import pandas as pd
import regex as re
from datetime import datetime
import os

def read_mxliff_file(file_path):
    with open(file_path, encoding='utf-8') as xml_file:
        data_dict = xmltodict.parse(xml_file.read())
    return data_dict

def extract_data(data_dict):
    list_groups = data_dict['xliff']['file']['body']['group']
    data_for_pd = [(i['@id'], i['@m:para-id'], i['trans-unit']) for i in list_groups]
    df = pd.DataFrame(data_for_pd, columns=['p_id', 'para_id', 'trans_unit'])
    return df

def timestamp_to_human_readable(timestamp_ms):
    timestamp_ms = int(timestamp_ms)
    timestamp = timestamp_ms / 1000  # Convert from milliseconds to seconds
    human_readable_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    return human_readable_time

# Ask the user for input file path
input_file_path = input("Enter the .mxliff file path: ")


# Check if the file exists
while not os.path.isfile(input_file_path):
    print("File does not exist.")
    input_file_path = input("Enter the .mxliff file path: ")


try:
    df = extract_data(read_mxliff_file(input_file_path))
    df['SOURCE'] = df.trans_unit.apply(lambda x: x['source'])
    df['TARGET'] = df.trans_unit.apply(lambda x: x['target'])
    df['GROSS_SCORE'] = df.trans_unit.apply(lambda x: float(x['@m:gross-score']))
    df['M_SCORE'] = df.trans_unit.apply(lambda x: float(x['@m:score']))
    df['MODIFIED_AT'] = df.trans_unit.apply(lambda x: x['@m:modified-at'])
    df['MODIFIED_AT'] = df['MODIFIED_AT'].apply(lambda x: timestamp_to_human_readable(x))
    df['LOCKED'] = df.trans_unit.apply(lambda x: x['@m:locked'])
    df['ALT_MATCH_QUALITY'] = df.trans_unit.apply(lambda x: round(float(x['alt-trans'][1]['@match-quality']), 2))
    df['ALT_TARGET'] = df.trans_unit.apply(lambda x: x['alt-trans'][1]['target'])
    df['CONFIRMED'] = df.trans_unit.apply(lambda x: x['@m:confirmed'])

    sample = df[(df.TARGET != r'\p') & (df.TARGET.apply(lambda x: x.startswith(r'\c') == False))].sample(frac=0.1)
    sample = sample[['SOURCE', 'TARGET']]

    # Ask the user for output file names
    html_output_file = input("Enter the HTML output file name: ")
    pickle_output_file = input("Enter the DataFrame pickle output file name: ")

    # Add file extensions if missing
    if not html_output_file.endswith('.html'):
        html_output_file += '.html'
    if not pickle_output_file.endswith('.pkl'):
        pickle_output_file += '.pkl'

    # Create the data for HTML content
    data = [f'''
<div class='akbar'>
    <p>{source}</p>
    <p>{target}</p>
</div><hr>''' 
for source, target in zip(sample['SOURCE'], sample['TARGET'])]
    data = ''.join(data)

    # Create the HTML content
    html_content = fr'''
    <!DOCTYPE html>
    <html lang="ar" dir="rtl">
      <head>
        <meta charset="UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <!-- Bootstrap style -->

<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@4.3.1/dist/css/bootstrap.min.css" integrity="sha384-ggOyR0iXCbMQv3Xipma34MD+dH/1fQ784/j6cY/iJTQUOhcWr7x9JvoRxT2MZw1T" crossorigin="anonymous">

        <title>Revise Translation</title>
        <style>
        .akbar {{
            margin: 1rem, auto;
            max-width: 1200px;
        }}

        p {{
    text-align: center;
}}
        </style>
      </head>
      <body dir="rtl">
  <div class="container-fluid vh-100 d-flex justify-content-center align-items-center">
    <div class="my-content">
    {data}
    </div>
  </div>


        <!-- Bootstrap scripts -->
<script src="https://code.jquery.com/jquery-3.3.1.slim.min.js" integrity="sha384-q8i/X+965DzO0rT7abK41JStQIAqVgRVzpbzo5smXKp4YfRvH+8abtTE1Pi6jizo" crossorigin="anonymous"></script>
<script src="https://cdn.jsdelivr.net/npm/popper.js@1.14.7/dist/umd/popper.min.js" integrity="sha384-UO2eT0CpHqdSJQ6hJty5KVphtPhzWj9WO1clHTMGa3JDZwrnQq4sF86dIHNDz0W1" crossorigin="anonymous"></script>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@4.3.1/dist/js/bootstrap.min.js" integrity="sha384-JjSmVgyd0p3pXB1rRibZUAYoIIy6OrQ6VrjIEaFf/nJGzIxFDsf4x0xIM+B07jRM" crossorigin="anonymous"></script>
      </body>
    </html>
    '''.strip()

    # Save the HTML content to a file
    with open(html_output_file, mode='w', encoding='utf-8') as f:
        f.write(html_content)

    # Save the DataFrame to a pickle file
    print(f'Total Number of Translation Units: {len(df)}')
    df.to_pickle(pickle_output_file, protocol=4)

    print(f"Files '{html_output_file}' and '{pickle_output_file}' created successfully!")

except Exception as e:
    print(f"An error occurred: {e}")
