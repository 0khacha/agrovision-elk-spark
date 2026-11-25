// AgroVision - analyse du graphe metier avec GraphX (Scala).
// Entrees : data/graph/vertices.csv (755 sommets), data/graph/edges.csv (2 879 aretes)
// Controles de construction (PASS/FAIL), puis 3 analyses :
//   A1 degres par type de sommet ; A2 PageRank et charge ponderee des sources d'eau ;
//   A3 composantes connexes (irrigation principale seule / avec secours)
// Sorties : results/graphx/ (CSV) et graphx_report.txt
// Usage   : spark-shell --driver-memory 1g -i agro_graph.scala   (puis :quit)
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{round, desc, col}
import spark.implicits._

case class VA(id: String, vtype: String, region: String, subtype: String, crop: String)
case class EA(rel: String, dist: Double, share: Double)

val H = sys.env("HOME") + "/tp6_elk"
val OUT = H + "/results/graphx"
new java.io.File(OUT).mkdirs()
@transient val log = new java.io.PrintWriter(new java.io.File(OUT + "/graphx_report.txt"))
def say(x: String): Unit = { println(x); log.println(x); log.flush() }
def table(df: DataFrame, n: Int = 20): Unit = {
  say(df.columns.mkString(" | "))
  df.limit(n).collect().foreach(r => say(r.toSeq.map(v => if (v == null) "NULL" else v.toString).mkString(" | ")))
}
def save(df: DataFrame, name: String): Unit =
  df.coalesce(1).write.mode("overwrite").option("header", "true").csv(OUT + "/" + name)
def chk(label: String, got: Any, ok: Boolean): Unit = say((if (ok) "PASS  " else "FAIL  ") + label + " -> " + got)

// ---------- Mission 5 : construction du graphe ----------
def vid(id: String): Long = {
  val n = id.substring(4).toLong
  id.take(3) match {
    case "FLD" => n
    case "WTR" => 10000L + n
    case "EXP" => 20000L + n
    case _ => throw new IllegalArgumentException("identifiant inconnu : " + id)
  }
}
def str(r: Row, c: String): String = Option(r.getAs[String](c)).getOrElse("")
def dbl(x: String): Double = if (x.isEmpty) 0.0 else x.toDouble

val vdf = spark.read.option("header", "true").csv(H + "/data/graph/vertices.csv")
val edf = spark.read.option("header", "true").csv(H + "/data/graph/edges.csv")
val vRows = vdf.rdd.map(r => (vid(str(r, "vertex_id")),
  VA(str(r, "vertex_id"), str(r, "vertex_type"), str(r, "region"), str(r, "subtype"), str(r, "crop_type")))).cache()
val eRows = edf.rdd.map(r => Edge(vid(str(r, "src_id")), vid(str(r, "dst_id")),
  EA(str(r, "relation"), dbl(str(r, "distance_km")), dbl(str(r, "share_pct"))))).cache()
val graph = Graph(vRows, eRows, VA("?", "?", "?", "?", "?")).cache()

say("===== MISSION 5 : CONSTRUCTION ET VERIFICATION DU GRAPHE =====")
val nV = graph.vertices.count(); val nE = graph.edges.count()
chk("Sommets >= 500", nV, nV >= 500)
chk("Aretes >= 2000", nE, nE >= 2000)
chk("Identifiants texte uniques", vRows.count() + " lignes / " + vRows.map(_._2.id).distinct.count() + " distincts",
  vRows.count() == vRows.map(_._2.id).distinct.count())
chk("Identifiants numeriques sans collision", vRows.map(_._1).distinct.count(), vRows.map(_._1).distinct.count() == vRows.count())
val unknown = graph.vertices.filter(_._2.vtype == "?").count()
chk("Aretes vers un sommet inconnu", unknown, unknown == 0)
val loops = eRows.filter(e => e.srcId == e.dstId).count()
chk("Boucles", loops, loops == 0)
val dupE = nE - eRows.map(e => (e.srcId, e.dstId, e.attr.rel)).distinct.count()
chk("Aretes en double", dupE, dupE == 0)
val badType = graph.triplets.filter { t =>
  val a = t.srcAttr.vtype; val b = t.dstAttr.vtype
  t.attr.rel match {
    case "VOISINE_DE" => !(a == "PARCELLE" && b == "PARCELLE")
    case "IRRIGUEE_PAR" => !(a == "PARCELLE" && b == "SOURCE_EAU")
    case "EXPLOITEE_PAR" => !(a == "PARCELLE" && b == "EXPLOITATION")
    case _ => true
  }
}.count()
chk("Aretes aux extremites de type non conforme", badType, badType == 0)
say("Sommets par type : " + graph.vertices.map(_._2.vtype).countByValue().toSeq.sorted.mkString(", "))
say("Aretes par relation : " + graph.edges.map(_.attr.rel).countByValue().toSeq.sorted.mkString(", "))

// ---------- Mission 6, analyse 1 : degres ----------
say("\n===== ANALYSE 1 : DEGRES =====")
val degDF = graph.vertices.join(graph.degrees).map { case (_, (a, dg)) => (a.id, a.vtype, a.region, a.subtype, a.crop, dg) }
  .toDF("sommet", "type", "region", "sous_type", "culture", "degre")
say("Degre par type de sommet :")
table(degDF.groupBy("type").agg(org.apache.spark.sql.functions.count("*").as("n"),
  round(org.apache.spark.sql.functions.avg("degre"), 2).as("degre_moyen"),
  org.apache.spark.sql.functions.min("degre").as("min"), org.apache.spark.sql.functions.max("degre").as("max")))
say("10 parcelles de plus fort degre (voisines + irrigation + exploitation) :")
val topPar = degDF.filter("type = 'PARCELLE'").orderBy(desc("degre"), col("sommet")).limit(10)
table(topPar); save(topPar, "analyse1_parcelles_degre")

// ---------- Irrigation : sous-graphe parcelles -> sources ----------
val irr = graph.subgraph(epred = t => t.attr.rel == "IRRIGUEE_PAR", vpred = (id, a) => a.vtype != "EXPLOITATION")
say("Parcelles par nombre de sources (secours) : " + irr.outDegrees.map(_._2).countByValue().toSeq.sorted.mkString(", "))

// ---------- Mission 6, analyse 2 : sources critiques ----------
say("\n===== ANALYSE 2 : SOURCES D'EAU CRITIQUES (PageRank et charge ponderee) =====")
val load = irr.aggregateMessages[Double](ctx => ctx.sendToDst(ctx.attr.share / 100.0), _ + _)
val pr = irr.pageRank(0.0001, 0.15).vertices
val srcDF = irr.vertices.filter(_._2.vtype == "SOURCE_EAU").map { case (id, a) => (id, a.id, a.region, a.subtype) }
  .toDF("k", "source", "region", "type")
val inDF = irr.inDegrees.map(x => (x._1, x._2)).toDF("k", "n_parcelles")
val loadDF = load.map(x => (x._1, x._2)).toDF("k", "parcelles_equiv")
val prDF = pr.map(x => (x._1, x._2)).toDF("k", "pagerank")
val srcTbl = srcDF.join(inDF, "k").join(loadDF, "k").join(prDF, "k")
  .select(col("source"), col("region"), col("type"), col("n_parcelles"),
    round(col("parcelles_equiv"), 2).as("parcelles_equiv"), round(col("pagerank"), 4).as("pagerank")).cache()
say("Sources avec au moins une parcelle : " + srcTbl.count() + " | sans parcelle : " + (srcDF.count() - srcTbl.count()))
say("Top 10 par PageRank :")
val topPR = srcTbl.orderBy(desc("pagerank"), col("source")).limit(10); table(topPR); save(topPR, "analyse2_top_pagerank")
say("Top 10 par charge ponderee :")
val topLoad = srcTbl.orderBy(desc("parcelles_equiv"), col("source")).limit(10); table(topLoad); save(topLoad, "analyse2_top_charge")
val s1 = topPR.select("source").collect().map(_.getString(0)).toSet
val s2 = topLoad.select("source").collect().map(_.getString(0)).toSet
say("Sources communes aux deux top 10 : " + (s1 intersect s2).size + " / 10")
val top3 = srcTbl.orderBy(desc("n_parcelles"), col("source")).limit(3).collect().map(r => r.getString(0) + ":" + r.getInt(3)).mkString(", ")
say("Controle croise Python (attendu WTR-009:34, WTR-020:31, WTR-038:29) : " + top3)
chk("Top 3 identique a l'apercu Python", top3, top3 == "WTR-009:34, WTR-020:31, WTR-038:29")
save(srcTbl.orderBy(desc("pagerank")), "analyse2_toutes_sources")

// ---------- Mission 6, analyse 3 : composantes connexes en deux couches ----------
say("\n===== ANALYSE 3 : SECTEURS HYDRAULIQUES (Connected Components) =====")
def layer(minShare: Double): Array[(Long, Int, Int, Int, Int)] = {
  val sg = graph.subgraph(epred = t => t.attr.rel == "IRRIGUEE_PAR" && t.attr.share >= minShare,
                          vpred = (id, a) => a.vtype != "EXPLOITATION")
  sg.connectedComponents().vertices.join(sg.vertices)
    .map { case (_, (c, a)) => (c, a) }
    .groupByKey()
    .map { case (c, xs) => (c, xs.size, xs.count(_.vtype == "SOURCE_EAU"), xs.count(_.vtype == "PARCELLE"), xs.map(_.region).toSet.size) }
    .collect()
}
def report(name: String, st: Array[(Long, Int, Int, Int, Int)]): Unit = {
  val withP = st.filter(_._4 > 0)
  say(name + " : composantes (toutes) = " + st.length + " | avec au moins une parcelle = " + withP.length)
  say("   composantes a >= 2 sources = " + st.count(_._3 >= 2) + " | composantes sur plusieurs regions = " + st.count(_._5 > 1))
  say("   5 plus grandes (parcelles, sources) : " + withP.sortBy(-_._4).take(5).map(x => "(" + x._4 + "," + x._3 + ")").mkString(" "))
  say("   parcelles couvertes = " + st.map(_._4).sum)
}
val stA = layer(60.0); val stB = layer(0.0)
report("Couche A (irrigation principale seule)", stA)
report("Couche B (principale + secours)", stB)
save(stA.toSeq.toDF("composante", "taille", "sources", "parcelles", "regions"), "analyse3_composantes_A")
save(stB.toSeq.toDF("composante", "taille", "sources", "parcelles", "regions"), "analyse3_composantes_B")
val rep = spark.read.option("multiLine", "true").json(H + "/data/graph/graph_report.json").first()
val pyIrr = rep.getAs[Long]("components_irrigation"); val pyFull = rep.getAs[Long]("components_full")
chk("Couche B = composantes irrigation de l'apercu Python", stB.length + " vs " + pyIrr, stB.length.toLong == pyIrr)
val fullCC = graph.connectedComponents().vertices.map(_._2).distinct.count()
chk("Graphe complet = composantes Python (6 regions)", fullCC + " vs " + pyFull, fullCC == pyFull)
say("\nFIN : rapport ecrit dans " + OUT + "/graphx_report.txt")
log.close()
sys.exit(0)
