## Les opérations uniquement que j'ai effectivement testées et timées

| opération  | elapsed time   | nombre de coeurs  | total CPU   |  type noeud | outils |
|---|---|---|---|---|---|
| filtrage spatial 100km sur 1 champs 2D eNATL60 24h | 14mn  |  240 | 120h  | HSW24  | jupyter notebook + dask-jobqueue  |
| filtrage temporel fréquence coriolis  sur 1 champs 2D eNATL60 24h | 3mn  |  240 | 120h  | HSW24  | jupyter notebook + dask-jobqueue  |
| profil w'b' moyen dans 9 boîtes 2°x2° 24h  | 28mn  |  240 | 120h  | HSW24  | jupyter notebook + dask-jobqueue  |
| zarrification champs 2D eNATL60 1 mois | 11mn  |  240 | 120h  | HSW24  | jupyter notebook + dask-jobqueue  |
| zarrification champs 3D eNATL60 24h  | 18mn  |  240 | 120h  | HSW24  | jupyter notebook + dask-jobqueue  |

(même consommation CPU car je demandais à chaque fois une demi-heure de 240 procs)
(pour info j'ai cramé 1800h cette semaine pour faire ces tests)

## Extrapolation pour 1 runs eNATL60 1 an de simulation horaire

J'essaie de parallèliser au max en multipliant le nombre de procs par 300 pour les champs 3D

| opération  | elapsed time   | nombre de coeurs  | total CPU   |  type noeud | outils |
|---|---|---|---|---|---|
| filtrage spatial 100km 1 champs 3D | 85h  | 72 000 | 6 132 000h  | HSW24  | jupyter notebook + dask-jobqueue  |
| filtrage spatial 100km 1 champs 2D | 85h | 240  |  20 400h | HSW24  | jupyter notebook + dask-jobqueue  |
| filtrage temporel fréquence coriolis  sur 1 champs 3D | 18h  |  72 000 | 1 314 000h  | HSW24  | 
| filtrage temporel fréquence coriolis  sur 1 champs 2D | 18h | 240 | 4 380h  | HSW24  | jupyter notebook + dask-jobqueue  |
| profil w'b' moyen dans 900 boîtes | 28mn  |  24 000 | 672 000h  | HSW24  | jupyter notebook + dask-jobqueue  |
| zarrification champs 2D eNATL60 | 2,2h  |  240 | 528h  | HSW24  | jupyter notebook + dask-jobqueue  |
| zarrification champs 3D eNATL60  | 170h  |  240 | 40 880h  | HSW24  | jupyter notebook + dask-jobqueue  |
| concaténation champs 2D eNATL60 1an  | ≈10h  | 48  | 0  |  visu interactive nodes | jupyter notebook + dask distributed  |
| concaténation champs 2D eNATL60 1an  | pas testé, sûrement trèèèès long | 48  | 0  |  visu interactive nodes | jupyter notebook + dask distributed  |


à multiplier par le nombre de variables variables 3D (u,v,w,t,s) et 2D (sst,sss,ssu,ssv,ssh) et le nombre de simulations 2

Total : 5 x ( 6 132 000 + 20 400 + 1 314 000 + 4 380 + 528 + 40 880 ) > 35 000 000h !!

A près j'imagine qu'on ne va pas s'amuser à filtrer les 100 derniers niveaux mais peut-etre on a besoin de profils et/ou sections qui vont jusqu'au fond ?

Il faut vraiment espérer que ça scale mieux que ça avec dask et les chunks ...