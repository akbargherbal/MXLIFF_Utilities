
import json
import xmltodict
import pandas as pd





with open("Mark-ar-ar_bh-T.mxliff", encoding='utf-8') as xml_file:
    data_dict = xmltodict.parse(xml_file.read())





import os





import regex as re





data_dict.keys()





type(data_dict['xliff']['file']['body']['group'])





len(data_dict['xliff']['file']['body']['group'])





list_groups = data_dict['xliff']['file']['body']['group']





[type(i) for i in  list_groups[322]]





list_groups[322]['trans-unit']





data_for_pd = [(i['@id'], i['@m:para-id'], i['trans-unit']) for i in list_groups]





df = pd.DataFrame(data_for_pd).rename(columns={0: 'p_id', 1: 'para_id', 2: 'trans_unit'})





idx = df.sample().index[0]





df['trans_unit'].iloc[idx]





df['LOCKED'] = df['trans_unit'].apply(lambda x: x['@m:locked'])





df['SRC_TXT'] = df['trans_unit'].apply(lambda x: x['source'].strip())





df[df['LOCKED'] == 'true']





dict_para_id_freq = {k:v for k,v in dict(df.para_id.value_counts()).items() if v>1}





len(dict_para_id_freq)





{'\\c', '\\p', '\\s1', '\\v'}





df = df.iloc[11:]





df = df.reset_index(drop=True)











def check_text_type(text):
    dict_types = {'\\c': 'CHAPTER', '\\p': 'PASSAGE', '\\s1': 'SECTION', '\\v': 'VERSE'}
    for key, value in dict_types.items():
        if text.startswith(key):
            return value
    return 'NO'





df['TYPE'] = df.SRC_TXT.apply(lambda x: check_text_type(x))

















def get_chapter_no(text):
    result = re.findall(r'^\\c.*?\d+', text)
    result = ''.join(result)
    
    if result:
        return result.split()[-1]
    else:
        return 'NO'





df['CHAPTER_NO'] = df.SRC_TXT.apply(lambda x: get_chapter_no(x))





df.CHAPTER_NO.value_counts()





#df.to_pickle('SRC_STAGE_01.pkl', protocol=4)











df['LOCKED'] = df['LOCKED'].apply(lambda x: 'YES' if x == 'true' else 'NO')





#df.to_pickle('SRC_STAGE_01.pkl', protocol=4)





df = df['p_id	para_id		LOCKED	SRC_TXT	TYPE	CHAPTER_NO'.split()]





#df.to_excel('SRC_STAGE_01.xlsx', encoding='utf-8', index=False)





df = pd.read_excel('SRC_STAGE_01.xlsx')





df[df['TYPE'] == 'NO']





df['TYPE'].value_counts()





test = df[df['TYPE']== 'PASSAGE']





df['SRC_TXT'].iloc[5]





test.SRC_TXT.value_counts()





123 + 220 





df['TA'].value_counts()





test = df[df['SRC_TXT'].apply(lambda x: len(x.split()) ==1)]





test['SRC_TXT'].value_counts()





def assert_passage(text, text_type):
    if (len(text.split()) > 1) and (text.startswith(r'\p')):
        print('IF')
        print('----------')
        return 'VERSE'
    else:
#         print('ELSE')
        return text_type











#del test, test_01, test_02





df['TYPE'] = df.apply(
lambda x: assert_passage(x['SRC_TXT'], x['TYPE']),
axis=1)





test = df[df['TYPE']== 'SECTION']





for i in test.SRC_TXT:
    print(i)





test_01 = pd.read_pickle('../STAGE_05.pkl')





test_01[test_01['IS_SECTION'] == 'SECTION_TITLE']





# del test, test_01





verse_pat = re.compile(r'^\\v.*?\d+')





df.SRC_TXT.apply(lambda x: verse_pat.findall(x))





def get_verse_no(text, verse_pat = verse_pat):
    result = verse_pat.findall(text)
    if result:
        result = ''.join(result)
        result = re.split(r'\\v', result)[-1]
        return result
    else:
        return 'NO'





df['VERSE_NO'] = df.SRC_TXT.apply(lambda x: get_verse_no(x))





ad_hoc_para_id = 14





df[df.para_id == ad_hoc_para_id]

















#df.to_pickle('SRC_STAGE_02.pkl', protocol=4)





df['TYPE'].value_counts()





test = df[df['VERSE_NO']== 'NO']





test = test[test['TYPE'] != 'NO']





#del test





ad_hoc_para_id = 14





#df.to_excel('SRC_STAGE_02.xlsx', encoding='utf-8', index=False)





df = pd.read_excel('SRC_STAGE_02.xlsx')





test = df[df.VERSE_NO == 0]





for i in test.SRC_TXT:
    print(i)





#df.to_pickle('SRC_STAGE_03.pkl', protocol=4)





df





df['CV'] = df.apply(lambda x: (x.CHAPTER_NO, x.VERSE_NO), axis=1)











dict_pay_attention = dict(df.CV.value_counts())





dict_pay_attention = {k:v for k,v in dict_pay_attention.items() if (v>1) and (k[1] != 0)}





df['PAY_ATTENTION'] = df.CV.apply(lambda x: 'YES' if x in dict_pay_attention else 'NO')

















len(df[df['SRC_TXT'].apply(lambda x: x.startswith('\c'))])





def pre_translate(x):
    if x.strip() == r'\p':
        return r'\p'
    elif x.strip().startswith(r'\c'):
        return x.strip()
    else:
        return ''





df['PRE_TRANSLATE'] =df['SRC_TXT'].apply(lambda x: pre_translate(x))





df





df











df['IDX'] = df.index
grouped = df.groupby('CHAPTER_NO')

# Step 2: Save each group into separate pickle files
for category, group_df in grouped:
    filename = f"./SPLIT_SRC/SRC_MARK_{str(category).zfill(2)}.pkl"
    group_df = group_df.sort_values(['IDX'])
    group_df.to_pickle(filename, protocol=4)





pd.read_pickle('SPLIT_SRC/SRC_MARK_01.pkl')





def convert_pkl2xlsx(path_file):
    df = pd.read_pickle(path_file)
    file_name = path_file.split('.')[0]
    df.to_excel(f'{file_name}.xlsx', encoding='utf-8', index=False)
    print(f'Successfully converted {path_file} to {file_name}.xlsx')





list_pkl_files = os.listdir('SPLIT_SRC/')





list_pkl_files = [f'SPLIT_SRC/{i}' for i in list_pkl_files]





convert_pkl2xlsx(list_pkl_files[0])





for (idx, pkl) in enumerate(list_pkl_files):
    print(f'Converting {pkl} of {len(list_pkl_files)}')
    convert_pkl2xlsx(pkl)
    print('---------------')



















