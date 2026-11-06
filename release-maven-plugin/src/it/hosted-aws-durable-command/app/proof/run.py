from pathlib import Path
import tempfile, zipfile, subprocess, os, sys
project=Path(__file__).resolve().parent.parent
java=Path(os.environ['JAVA_HOME'])/'bin'
with tempfile.TemporaryDirectory(prefix='tpf-command-package-') as temp:
    root=Path(temp); package=root/'package'; package.mkdir(); classes=root/'proof'; classes.mkdir()
    with zipfile.ZipFile(project/'target/function.zip') as archive: archive.extractall(package)
    cp=os.pathsep.join([str(package),str(package/'lib/*'),str(package/'*')])
    sources=list((project/'proof').glob('*.java'))
    subprocess.run([str(java/'javac'),'-proc:none','-cp',cp,'-d',str(classes),*map(str,sources)],check=True)
    subprocess.run([str(java/'java'),'-cp',str(classes)+os.pathsep+cp,'org.pipelineframework.aws.durable.HostCommandProof',sys.argv[1]],check=True,cwd=package,timeout=50)
