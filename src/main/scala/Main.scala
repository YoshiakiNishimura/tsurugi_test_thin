import com.tsurugidb.iceaxe.TsurugiConnector
import com.tsurugidb.iceaxe.metadata.TgTableMetadata
import com.tsurugidb.iceaxe.session.TsurugiSession
import com.tsurugidb.iceaxe.sql.{TsurugiSqlStatement, TsurugiSqlQuery}
import com.tsurugidb.iceaxe.sql.result.{
  TsurugiResultEntity,
  TsurugiStatementResult,
  TsurugiQueryResult
}
import com.tsurugidb.iceaxe.transaction.manager.{
  TgTmSetting,
  TsurugiTransactionManager
}
import com.tsurugidb.iceaxe.transaction.option
import com.tsurugidb.iceaxe.transaction.option.TgTxOption
import com.tsurugidb.iceaxe.transaction.option.TgTxOptionLtx
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction

import com.tsurugidb.tsubakuro.common.{Session, SessionBuilder}
import com.tsurugidb.tsubakuro.sql.{SqlClient, Transaction}
import com.tsurugidb.tsubakuro.kvs.{KvsClient, RecordBuffer, TransactionHandle}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext}
import scala.jdk.CollectionConverters._
import scala.util.{Using, Try, Success, Failure}
import java.net.URI

private val Connect = "ipc://tsurugi2"
private val TableName = "test_table"
private val TableName2 = "test_table2"
private val Columncount = 16000000 - 2
private val startHex = 0x1000000
private val endHex = 0x3000000
class Setting(val tg: TgTmSetting, val name: String) {
  def getName: String = name
  def getTgTmSetting: TgTmSetting = tg
}

class Table(
    val tableName: String,
    val format: String,
    val rowCount: Int,
    val columnCount: Int
) {
  def getTableName: String = tableName
  def getFormat: String = format
  def getRowCount: Int = rowCount
  def getColumnCount: Int = columnCount

  def createRecordBuffer(id: Int): RecordBuffer = {
    val record = new RecordBuffer()
    record.add("id", id)
    record.add("name", id)
    record.add("note", id)
  }
}
def insert(kvs: KvsClient, table: Table)(implicit
    ec: ExecutionContext
): Unit = {
  println(s"insert ${table.getTableName} column ${table.getColumnCount}")
  Try {
    val tx = kvs.beginTransaction().await
    val range = (0 until Columncount).map(startHex + _).toList
    range.foreach { i =>
        val record = table.createRecordBuffer(i)
        kvs.put(tx, table.getTableName, record).await
    }
    val record_start = table.createRecordBuffer(0)
    kvs.put(tx, table.getTableName, record_start).await
    val record_end = table.createRecordBuffer(endHex)
    kvs.put(tx, table.getTableName, record_end).await
    kvs.commit(tx).await
    tx.close()
  } recover { case e: Exception =>
    println(e.getMessage)
  }
}

def dropCreate(sql: SqlClient, t: Table)(implicit
    ec: ExecutionContext
): Unit = {
  val drop = s"DROP TABLE ${t.getTableName}"
  val create = s"CREATE TABLE ${t.getTableName} ${t.getFormat}"

  println(s"${drop}")
  Try {
    val transaction = sql.createTransaction().await
    transaction.executeStatement(drop).await
    transaction.commit().await
    transaction.close()
  } recover { case e: Exception =>
    println(e.getMessage)
  }

  println(s"${create}")
  Try {
    val transaction = sql.createTransaction().await
    transaction.executeStatement(create).await
    transaction.commit().await
    transaction.close()
  } recover { case e: Exception =>
    println(e.getMessage)
  }
}

def sqlExecute(
    session: Session,
    sql: SqlClient,
    kvs: KvsClient,
    table: Table
): Unit = {
  val createAndInsertTime = System.nanoTime()
  dropCreate(sql, table)
  insert(kvs, table)
  val createAndInsertEndTime = System.nanoTime()
  println(
    s"createAndInsert ${(createAndInsertEndTime - createAndInsertTime) / 1_000_000} ms"
  )
}

def executeSelect(session: TsurugiSession, setting: Setting): Unit = {
  println(setting.getName)
  val sql = s"delete from $TableName"
  val start = System.nanoTime()
  val tm = session.createTransactionManager(setting.getTgTmSetting)
  tm.executeAndGetCountDetail(sql);
  val end = System.nanoTime()
  println(s"${(end - start) / 1_000_000} ms")
}

def executeAndClose(
    sql: SqlClient,
    kvs: KvsClient,
    session: Session,
    table: Table
): Unit = {
  sqlExecute(session, sql, kvs, table)
//  sql.close()
//  kvs.close()
//  session.close()
}


def using[T <: AutoCloseable, R](resource: T)(f: T => R): R =
  try f(resource)
  finally resource.close()

@main def run(): Unit = {
  println("start")
  val endpoint = URI.create(Connect)
  val connector = TsurugiConnector.of(endpoint)
  val table = new Table(
    TableName,
    "(id int primary key, name int, note int)",
    3,
    Columncount
  )
  val table2 = new Table(
    TableName2,
    "(id int primary key, name int, note int)",
    3,
    Columncount
  )
  (1 to 1).foreach { i =>
  println(s" strat ${i} times");
  Using.Manager { use =>
    implicit val ec: ExecutionContext = ExecutionContext.global
    val session = use(SessionBuilder.connect(endpoint).create())
    val sql = use(SqlClient.attach(session))
    val kvs = use(KvsClient.attach(session))
    executeAndClose(sql, kvs, session, table)
    session.close()
  } match {
    case Success(_)         =>
    case Failure(exception) => println(s"error : ${exception.getMessage}")
  }
  }
}
