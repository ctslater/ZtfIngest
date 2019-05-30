
This is used only to build a reproducible version of sbt/scala on a fixed java
version. Usage:

```
docker build -t scala-sbt docker
docker run -it --rm  -v `pwd`:/root  scala-sbt sbt
```
