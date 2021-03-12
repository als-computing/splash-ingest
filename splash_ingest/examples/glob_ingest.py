import glob
from ingest_scicat import gen_ev_docs, ScicatTomo

files = glob.glob('/global/cfs/projectdirs/als/data_mover/8.3.2/raw/dyparkinson/*ddd*.h5')
mapping_file = '../../mappings/832Mapping.json'
data_group = 'als_832'
scicat_tomo = ScicatTomo()

for file in files:
    print(file)
    gen_ev_docs(scicat_tomo, mapping_file, file, data_group)


