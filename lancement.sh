# Définition des constantes
PATH_REPO='~/Bureau/S8/hidoop/src'
SUFFIX='.enseeiht.fr'
CLIENT='targaryen'${SUFFIX}
DAEMON1='r2d2'${SUFFIX}
DAEMON2='hydre'${SUFFIX}
DAEMON3='pikachu'${SUFFIX}
NAMENODE='targaryen'${SUFFIX}

#"r2d2.enseeiht.fr", "nickel.enseeiht.fr", "pikachu.enseeiht.fr"

echo "Lancement du namenode"
ssh ftoubali@${NAMENODE} "cd ${PATH_REPO} && java hdfs.NameNodeImpl"&
sleep 5
echo "Lancement des HDFSservers"
ssh ftoubali@${DAEMON1} "pkill java && cd ${PATH_REPO} && java hdfs.HdfsServer 8001"&
ssh ftoubali@${DAEMON2} "pkill java && cd ${PATH_REPO} && java hdfs.HdfsServer 8002"&
ssh ftoubali@${DAEMON3} "pkill java && cd ${PATH_REPO} && java hdfs.HdfsServer 8003"&
sleep 5
echo "Lancement des daemons"
ssh ftoubali@${DAEMON1} "cd ${PATH_REPO} && java ordo.WorkerImpl 8887"&
ssh ftoubali@${DAEMON2} "cd ${PATH_REPO} && java ordo.WorkerImpl 8888"&
ssh ftoubali@${DAEMON3} "cd ${PATH_REPO} && java ordo.WorkerImpl 9000"&
sleep 5

echo  Lancement du client
ssh ftoubali@${CLIENT} "cd ${PATH_REPO} && java hdfs.HdfsClient write line filesample.txt"&
ssh ftoubali@${CLIENT} "cd ${PATH_REPO} && java application.MyMapReduce filesample.txt"



