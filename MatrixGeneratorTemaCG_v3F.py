print('>> Loading libraries...')

from pandas    import read_csv,concat,DataFrame
from numpy     import array
from itertools import product

from io import StringIO
from simplecrypt import encrypt, decrypt
from getpass import getpass

zonas=[[0, 292],
 [1, 341],
 [2, 2006],
 [3, 6005],
 [4, 6001],
 [5, 4005],
 [6, 4001],
 [7, 2001],
 [8, 2002],
 [9, 6002],
 [10, 2003],
 [11, 6003],
 [12, 6004],
 [13, 2004],
 [14, 4002],
 [15, 3002],
 [16, 5002],
 [17, 4003],
 [18, 2005],
 [19, 4004],
 [20, 132],
 [21, 3001],
 [22, 347],
 [23, 392],
 [24, 355],
 [25, 1373],
 [26, 1287],
 [27, 1288],
 [28, 131],
 [29, 140],
 [30, 192],
 [31, 210],
 [32, 361],
 [33, 380],
 [34, 1251],
 [35, 1281],
 [36, 1371],
 [37, 1400],
 [38, 5001],
 [39, 1345],
 [40, 1341],
 [41, 100],
 [42, 111],
 [43, 391],
 [44, 302],
 [45, 311],
 [46, 281],
 [47, 351],
 [48, 1290],
 [49, 312],
 [50, 1270],
 [51, 1284],
 [52, 1299],
 [53, 353],
 [54, 274],
 [55, 322],
 [56, 122],
 [57, 90],
 [58, 151],
 [59, 1295],
 [60, 1327],
 [61, 1332],
 [62, 1323],
 [63, 370],
 [64, 1331],
 [65, 1342],
 [66, 1344],
 [67, 1272],
 [68, 291],
 [69, 349],
 [70, 348],
 [71, 346]]
zonas_index  = [ x[0] for x in zonas ]
zonas_Zona18 = [ x[1] for x in zonas ]
zonas=DataFrame(data=zonas_Zona18,index=zonas_index)

indata=read_csv('./input_parameters.csv', sep=',',encoding='utf-8',header=None,index_col=0).transpose()

dates = ['20180507','20180508','20180509','20180510','20180511','20180512','20180513']
factor=1
if (indata.dia[1].strip() in dates):
    dates = [indata.dia[1].strip()]
    print('Loading '+indata.dia[1].strip())
elif indata.dia[1].strip()=='media':
    dates=['20180508','20180509','20180510']
    factor=3
    print('Loading average day')
else:
    print('Loading all days')

print('>> Reading data...')
daat = concat([
    read_csv(
    StringIO(decrypt('culo',open('./data/ODzonas_trips_r-35_'+ss+'.csv.enc','rb').read()).decode('latin'))
    , sep=';',encoding='utf-8',index_col=None) 
    for ss in dates])
daat['Domicilio'] = daat['Domicilio'].apply(lambda r: r[:4])

print(len(daat))

domicilios=['LasP','Gran','Extr','Espa']
domicilio=indata.domicilio[1].strip()[:4]
if domicilio in domicilios:
    daat=daat[daat['Domicilio']==domicilio]

print(len(daat))

if domicilio in domicilios[:2]:
    edad=eval(indata.edades[1].strip())
    daat=daat[(edad[0]<=daat['ID_EDAD'])&(daat['ID_EDAD']<=edad[1])]
    
    motivos=['GoWork','GoHome','GoBoth','GoAny']
    motivo=indata.motivo[1].strip()
    if motivo in motivos:
        daat=daat[daat['Reason']==motivo]
        
horas=eval(indata.horas[1].strip())
daat=daat[(horas[0]*10000<=daat['TimeO'])&(daat['TimeO']<=horas[1]*10000)]

print(len(daat),daat.W.sum())

daat=daat[['W','OD_zonas']]

allcombinatons=array(list(product(zonas[0],zonas[0]))).T
matrixOD=DataFrame({'origin' :allcombinatons[0],
                           'destination':allcombinatons[1],
                           'matrix_element':[0 for x in range(len(allcombinatons[0]))]})
matrixOD.set_index(['origin', 'destination'], inplace=True)
matrixODW=DataFrame({'origin' :allcombinatons[0],
                           'destination':allcombinatons[1],
                           'matrix_element':[0 for x in range(len(allcombinatons[0]))]})
matrixODW.set_index(['origin', 'destination'], inplace=True)

for ODZ,WW in zip(daat.OD_zonas,daat.W):
    lista=eval(ODZ)
    for odw in lista:
        matrixODW.loc[zonas.loc[odw[0],0],zonas.loc[odw[1],0]] += odw[2]*WW/factor
        matrixOD.loc[zonas.loc[odw[0],0],zonas.loc[odw[1],0]]  += odw[2]
		
filename=indata.NombreFiltro[1].strip()

matrixOD.to_csv('matrices/'+filename+'_matrixOD.csv', sep=',', encoding='utf-8')
print('matrixOD   sum:',matrixOD.sum().sum())
matrixODW.to_csv('matrices/'+filename+'_matrixOD_scaled.csv', sep=',', encoding='utf-8')
print('matrixOD_S sum:',matrixODW.sum().sum())

def gra(row,line):
    vv=(row.T).rename(index={'matrix_element': line})
    vv.index.names = ['']
    vv.columns.names = ['']
    return vv

mm=concat([ gra(matrixOD.loc[(ll,)],ll) for ll in zonas[0] ])
mm.to_csv('matrices/'+filename+'matrixOD_MM.csv', sep=',', encoding='utf-8')
print('MM   sum:      ',mm.sum().sum())
mmW=concat([ gra(matrixODW.loc[(ll,)],ll) for ll in zonas[0] ])
mmW.to_csv('matrices/'+filename+'matrixOD_MM_Scaled.csv', sep=',', encoding='utf-8')
print('MM_S sum:      ',mmW.sum().sum())


