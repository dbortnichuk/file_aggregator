package com.cisco.mantl.aggregator

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkContext

/**
 * Created by dbort on 06.10.2015.
 */
object FileGen {


  def main(args: Array[String]) {

    //debugCountdown(6)


    val sparkContext = new SparkContext("spark://quickstart.cloudera:7077", "FileGen")
    //val root = "/user/examples1/files-lot"
    val root = args(0)
    for( i <- 1 to args(1).toInt) {
      val filePath = root + "/file-" + i
      val fs = FileSystem.get(URI.create(filePath), sparkContext.hadoopConfiguration)
      val out = fs.create(new Path(filePath), true)
      val sb = new StringBuilder
      out.writeBytes(sb
        .append("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%").append(filePath).append("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
        .append(text).append(text).append(text).append(text).append(text).append(text).toString())
      out.close()
    }
    println("END")


  }

  val text = "THE TRAGEDY OF ROMEO AND JULIET by William Shakespeare Dramatis Personae   Chorus.   Escalus, Prince of Verona.   Paris, a young Count, kinsman to the Prince.   Montague, heads of two houses at variance with each other.   Capulet, heads of two houses at variance with each other.   An old Man, of the Capulet family.   Romeo, son to Montague.   Tybalt, nephew to Lady Capulet.   Mercutio, kinsman to the Prince and friend to Romeo.\n\n  Benvolio, nephew to Montague, and friend to Romeo\n\n  Tybalt, nephew to Lady Capulet.\n\n  Friar Laurence, Franciscan.   Friar John, Franciscan.   Balthasar, servant to Romeo.   Abram, servant to Montague.  Sampson, servant to Capulet.   Gregory, servant to Capulet.   Peter, servant to Juliet's nurse.   An Apothecary.   Three Musicians.   An Officer.   Lady Montague, wife to Montague.   Lady Capulet, wife to Capulet.   Juliet, daughter to Capulet.   Nurse to Juliet.   Citizens of Verona; Gentlemen and Gentlewomen of both houses;     Maskers, Torchbearers, Pages, Guards, Watchmen, Servants, and   Attendants.                          SCENE.--Verona; Mantua.                        THE PROLOGUE                     Enter Chorus. Chor. Two households, both alike in dignity,    In fair Verona, where we lay our scene,   From ancient grudge break to new mutiny,    Where civil blood makes civil hands unclean.   From forth the fatal loins of these two foes    A pair of star-cross'd lovers take their life;    Whose misadventur'd piteous overthrows    Doth with their death bury their parents' strife.  The fearful passage of their death-mark'd love,    And the continuance of their parents' rage,   Which, but their children's end, naught could remove,    Is now the two hours' traffic of our stage;   The which if you with patient ears attend,   What here shall miss, our toil shall strive to mend.                                                     [Exit.]"

}
