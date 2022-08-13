package site.ycsb.db.leveldb;

import net.jcip.annotations.GuardedBy;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

/**
 * LevelDB binding for <a href="https://github.com/fusesource/leveldbjni/">LevelDB</a>.
 * <p>
 * See {@code leveldb/README.md} for details.
 */
public class LevelDBClient extends DB {

  static final String PROPERTY_LEVELDB_DIR = "leveldb.dir";

  @GuardedBy("LevelDBClient.class")
  private static org.iq80.leveldb.DB levelDb = null;
  @GuardedBy("LevelDBClient.class")
  private static Path levelDbDir = null;

  private static final Logger LOGGER = LoggerFactory.getLogger(LevelDBClient.class);


  private synchronized void getDBInstance() throws DBException {
    levelDbDir = Paths.get(getProperties().getProperty(PROPERTY_LEVELDB_DIR));

    if (levelDb == null) {
      Options options = new Options();
      options.createIfMissing(true);
      try {

        // Create temp directory for levelDB
        if (!Files.exists(levelDbDir)) {
          Files.createDirectories(levelDbDir);
        }

        levelDb = factory.open(new File(levelDbDir.toAbsolutePath().toString()), options);
      } catch (IOException e) {
        LOGGER.error("Failed to open database");
        throw new DBException(e);
      }
    }
  }

  private static byte[] mapToBytes(Map<String, String> map) throws IOException {
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(byteOut);
    out.writeObject(map);
    return byteOut.toByteArray();
  }


  private static Map<String, String> bytesToMap(byte[] bytes) throws IOException, ClassNotFoundException {
    ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
    ObjectInputStream in = new ObjectInputStream(byteIn);
    @SuppressWarnings("unchecked") Map<String, String> map = (Map<String, String>) in.readObject();
    return map;
  }

  @Override
  public void init() throws DBException {
    try {
      getDBInstance();
    } catch (final org.iq80.leveldb.DBException e) {
      LOGGER.error(e.getMessage(), e);
      throw new DBException(e);
    }
  }

  @Override
  public void cleanup() throws DBException {

    try {
      levelDb.close();
    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);
      throw new DBException(e);
    }

  }


  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {

    byte[] value = levelDb.get(key.getBytes());
    if (value == null) {
      return Status.ERROR;
    }

    Map<String, String> map;
    try {
      map = bytesToMap(value);
    } catch (IOException | ClassNotFoundException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
    StringByteIterator.putAllAsByteIterators(result, map);
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

    DBIterator iterator = levelDb.iterator();
    int count = 0;
    try {
      iterator.seek(startkey.getBytes());
      while (iterator.hasNext() && count < recordcount) {
        String key = iterator.peekNext().getKey().toString();
        if (fields == null || fields.contains(key)) {
          HashMap<String, String> value;
          value = (HashMap<String, String>) bytesToMap(iterator.peekNext().getValue());
          HashMap<String, ByteIterator> byteValues = new HashMap<String, ByteIterator>();
          StringByteIterator.putAllAsByteIterators(byteValues, value);
          result.addElement(byteValues);
        }
        iterator.next();
        count += 1;
      }
    } catch (IOException | ClassNotFoundException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    } finally {
      try {
        iterator.close();
      } catch (IOException e) {
        LOGGER.error(e.getMessage(), e);
      }
    }
    return Status.OK;

  }


  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {

    byte[] existingBytes = levelDb.get(key.getBytes());
    Map<String, String> existingValues;
    if (existingBytes != null) {
      try {
        existingValues = bytesToMap(existingBytes);
      } catch (IOException | ClassNotFoundException e) {
        LOGGER.error(e.getMessage(), e);
        return Status.ERROR;
      }
    } else {
      existingValues = new HashMap<>();
    }
    Map<String, String> newValues = StringByteIterator.getStringMap(values);
    existingValues.putAll(newValues);
    try {
      levelDb.put(key.getBytes(), mapToBytes(existingValues));
    } catch (org.iq80.leveldb.DBException | IOException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
    return Status.OK;
  }


  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    Map<String, String> stringValues = StringByteIterator.getStringMap(values);
    try {
      levelDb.put(key.getBytes(), mapToBytes(stringValues));
      return Status.OK;
    } catch (org.iq80.leveldb.DBException | IOException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }


  @Override
  public Status delete(String table, String key) {
    try {
      levelDb.delete(key.getBytes());
      return Status.OK;
    } catch (org.iq80.leveldb.DBException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }
}
