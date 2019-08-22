package java.client;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.QueryCursor;


public class BankSql extends Client {
    private IgniteCache<Integer, Integer> cache;
    private static final String CACHE_NAME = Bank.class.getSimpleName();
    SqlFieldsQuery createQuery = new SqlFieldsQuery(
        "CREATE TABLE ACCOUNTS(id int PRIMARY KEY, balance int) WITH \"TEMPLATE=PARTITIONED, BACKUPS=3, ATOMICITY=TRANSACTIONAL, WRITE_SYNCHRONIZATION_MODE=FULL_ASYNC\"");
    SqlFieldsQuery selectQuery = new SqlFieldsQuery("SELECT balance FROM ACCOUNTS WHERE id = ?");
    SqlFieldsQuery insertQuery = new SqlFieldsQuery(
        "INSERT INTO ACCOUNTS (id,  balance) VALUES (?, ?)");
    SqlFieldsQuery indexQuery = new SqlFieldsQuery("CREATE INDEX account_idx ON ACCOUNTS (id)");
    SqlFieldsQuery updateQuery = new SqlFieldsQuery("UPDATE ACCOUNTS SET balance = ? WHERE id = ?");
    SqlFieldsQuery getAllQuery = new SqlFieldsQuery("SELECT balance FROM ACCOUNTS");

    public BankSql(String igniteConfig) {
        super(igniteConfig);
    }

    public void setAccountCache(CacheAtomicityMode            atomicityMode,
        CacheMode                     cacheMode,
        CacheWriteSynchronizationMode writeSynchronizationMode,
        Boolean                       readFromBackup,
        int                           backups) {
        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>(CACHE_NAME);
        cfg.setAtomicityMode(atomicityMode);
        cfg.setCacheMode(cacheMode);
        cfg.setWriteSynchronizationMode(writeSynchronizationMode);
        cfg.setReadFromBackup(readFromBackup);
        cfg.setBackups(backups);
        cache = ignite.getOrCreateCache(cfg);
        createQuery.setSchema("PUBLIC");
        selectQuery.setSchema("PUBLIC");
        insertQuery.setSchema("PUBLIC");
        updateQuery.setSchema("PUBLIC");
        getAllQuery.setSchema("PUBLIC");
        indexQuery.setSchema("PUBLIC");
        cache.query(createQuery);
        cache.query(indexQuery);
    }

    public int transferMoney(int                    fromAccountId,
        int                    toAccountId,
        TransactionConcurrency transactionConcurrency,
        TransactionIsolation   transactionIsolation) {

        // Starting the transaction and setting the timeout in order to defreeze the transaction if it gets into
        // the deadlock and to trigger the deadlock detection to troubleshoot the issue.
        // 10 (txSize) is an approximate number of entries participating in transaction.
        Transaction tx = ignite.transactions().txStart(transactionConcurrency, transactionIsolation, TX_TIMEOUT, 10);

        try {
            int fromAccount = getBalance(fromAccountId);
            int toAccount = getBalance(toAccountId);

            // No money in the account
            if (fromAccount < 1) {
                tx.commit();
                return 0;
            }

            int amount = getRandomNumberInRange(1, fromAccount);

            // Withdraw from account
            fromAccount = fromAccount - amount;

            // Deposit into account.
            toAccount = toAccount + amount;

            // Store updated accounts in cache.
            updateAccountBalance(fromAccountId, fromAccount);
            updateAccountBalance(toAccountId, toAccount);

            tx.commit();
            return amount;
        } catch (IgniteException ex){
            tx.rollback();
            throw ex;
        }
    }

    public void updateAccountBalance(int accountId, int balance) {
        cache.query(updateQuery.setArgs(balance, accountId));
    }

    public int getBalance(int accountId) {
        FieldsQueryCursor cursor = cache.query(selectQuery.setArgs(accountId));
        Iterator<ArrayList<Integer>> iterator = cursor.iterator();
        Integer result = null;
        if (iterator.hasNext()) {
            result = iterator.next().get(0);
        }
        return result;
    }

    private static int getRandomNumberInRange(int min, int max) {
        if (min > max) {
            throw new IllegalArgumentException("max (" + max + ") must be greater than min (" + min + ")");
        }
        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;
    }

    public Map<Integer, Integer> getAllAccounts(int endIndex,
        TransactionConcurrency transactionConcurrency,
        TransactionIsolation transactionIsolation) {
        FieldsQueryCursor cursor = cache.query(getAllQuery);
        Iterator<ArrayList<Integer>> iterator = cursor.iterator();
        List<Integer> keys = new ArrayList<Integer>();
        while (iterator.hasNext()) {
            keys.add(iterator.next().get(0));
        }
        return keys.stream().collect(Collectors.toMap(a -> 1, a -> a));
    }

    public void destroyCache() {
        ignite.destroyCache(CACHE_NAME);
    }
}
