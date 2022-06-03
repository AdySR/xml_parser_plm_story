import string
from numpy import dtype
import pandas as pd
import xml.etree.ElementTree as ET
import io

xmlFile = r'C:\Projects\xmlParser\file.xml'

def get_claim_number(xmlFile):
    attr = xmlFile.attrib

    for xml in xmlFile.iter('claim'):
        doc_dict = attr.copy()
        doc_dict.update(xml.attrib)
        doc_dict['data'] = xml.text

        yield doc_dict

def xmlFileDF_policy_number(xmlFile):
    attr = xmlFile.attrib

    for xml in xmlFile.iter('policy_number'):
        doc_dict = attr.copy()
        doc_dict.update(xml.attrib)
        doc_dict['data'] = xml.text

        yield doc_dict

def xmlFileDF_loss_date(xmlFile):
    attr = xmlFile.attrib

    for xml in xmlFile.iter('loss_date'):
        doc_dict = attr.copy()
        doc_dict.update(xml.attrib)
        doc_dict['data'] = xml.text

        yield doc_dict

def xmlFileDF_insured_name(xmlFile):
    attr = xmlFile.attrib

    for xml in xmlFile.iter('insured_name'):
        doc_dict = attr.copy()
        doc_dict.update(xml.attrib)
        doc_dict['data'] = xml.text

        yield doc_dict


etree = ET.parse(xmlFile)
xmlFileDF_claim = pd.DataFrame(list(get_claim_number(etree.getroot())))
xmlFileDF_policy_number = pd.DataFrame(list(xmlFileDF_policy_number(etree.getroot())))
xmlFileDF_loss_date = pd.DataFrame(list(xmlFileDF_loss_date(etree.getroot())))
xmlFileDF_insured_name = pd.DataFrame(list(xmlFileDF_insured_name(etree.getroot())))

table = pd.concat([xmlFileDF_claim, xmlFileDF_policy_number, xmlFileDF_loss_date, xmlFileDF_insured_name], axis=1)
table.columns = ['ClaimNumber','PolicyNumber','loss_date','insured_name']
table = table.infer_objects()

# table = table.withColumn('ClaimNumber', 'ClaimNumber'.cast(string()))
print(table[29:40])

table.to_csv(r'C:\Projects\xmlParser\Ouput.csv')

# print(table2[29:40])


# # converting datatypes
# df = df.infer_objects()
# print(df.dtypes)

