(ns jepsen.dgraph.bank
  "Implements a bank-account test, where we transfer amounts between a pool of
  accounts, and verify that reads always see a constant amount."
  (:require [clojure.tools.logging :refer [info]]
            [clojure.string :as str]
            [dom-top.core :refer [disorderly with-retry assert+]]
            [jepsen.dgraph [client :as c]]
            [jepsen [client :as client]
                    [generator :as generator]]
            [jepsen.tests.bank :as bank])
  (:import (io.dgraph TxnConflictException)))

(def pred-count "Number of predicates to stripe keys and values across."
  3)

(defn multi-pred-acct->key+amount
  "Takes a query result like {:key_0 1 :amount_2 5} and returns [1 5], by
  pattern-matching key_ and amount_ prefixes."
  [record]
  (reduce
    (fn [[key amount] [pred value]]
      (condp re-find (name pred)
        #"^key_" (do (assert (not key)
                             (str "Record " (pr-str record)
                                  " contained unexpected multiple keys!"))
                     [value amount])
        #"^amount_" (do (assert (not amount)
                                (str "Record " (pr-str record)
                                     " contained unexpected multiple amounts!"))
                        [key value])
        [key amount]))
    [nil nil]
    record))

(defn read-accounts
  "Given a transaction, reads all accounts. If a type predicate is provided,
  finds all accounts where that type predicate is \"account\". Otherwise, finds
  all accounts across all type predicates. Returns a map of keys to amounts."
  ; All predicates
  ([t]
   (->> (c/gen-preds "type" pred-count)
        (map (partial read-accounts t))
        (reduce merge)))
  ; One predicate in particular
  ([t type-predicate]
   (let [q (str "{ q(func: eq(" type-predicate ", $type)) {\n"
                (->> (concat (c/gen-preds "key"    pred-count)
                             (c/gen-preds "amount" pred-count))
                     (str/join "\n"))
                "}}")]
     (->> (c/query t q {:type "account"})
          :q
          ;((fn [x] (info :read-val (pr-str x)) x))
          (map multi-pred-acct->key+amount)
          (into (sorted-map))))))

(defn find-account
  "Finds an account by key. Returns an empty account when none exists."
  [t k]
  (let [kp (c/gen-pred "key"    pred-count k)
        ap (c/gen-pred "amount" pred-count k)]
    (let [r (-> (c/query t
                         (str "{ q(func: eq(" kp ", $key)) { uid "
                              kp " " ap " } } ")
                         {:key k})
                :q
                first)]
      (if r
        ; Note that we need :type for new accounts, but don't want to update it
        ; normally.
        {:uid    (:uid r)
         :key    (get r (keyword kp))
         :amount (get r (keyword ap))}
        ; Default account object when none exists
        {:key     k
         :type    "account"
         :amount  0}))))

(defn write-account!
  "Writes back an account map."
  [t account]
  (if (zero? (:amount account))
    (c/delete! t (assert+ (:uid account)))
    (let [k (assert+ (:key account))
          kp (c/gen-pred "key"    pred-count k)
          ap (c/gen-pred "amount" pred-count k)
          tp (c/gen-pred "type"   pred-count k)]
      (c/mutate! t (-> account
                       (select-keys [:uid])
                       (assoc (keyword tp) (:type account))
                       (assoc (keyword kp) (:key account))
                       (assoc (keyword ap) (:amount account)))))))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node)))

  (setup! [this test]
    ; Set up schemas
    (->> (concat
           ; Key schemas
           (map (fn [pred]
                  (str pred ": int @index(int)"
                       (when (:upsert-schema test)
                         " @upsert")
                       " .\n"))
                (c/gen-preds "key" pred-count))

           ; Type schemas
           (map (fn [pred]
                  (str pred ": string @index(exact) .\n"))
                (c/gen-preds "type" pred-count))

           ; Amount schemas
           (map (fn [pred]
                  (str pred ": int .\n"))
                (c/gen-preds "amount" pred-count)))
         str/join
         (c/alter-schema! conn))
    (info "Schema altered")

    ; Insert initial value
    (try
      (c/with-txn test [t conn]
        (let [k (first (:accounts test))
              tp (keyword (c/gen-pred "type"    pred-count k))
              kp (keyword (c/gen-pred "key"     pred-count k))
              ap (keyword (c/gen-pred "amount"  pred-count k))
              r  {kp k
                  tp "account",
                  ap (:total-amount test)}]
          (info "Upserting" r)
          (c/upsert! t kp r)))
      (catch TxnConflictException e)))

  (invoke! [this test op]
    (c/with-conflict-as-fail op
      (c/with-txn test [t conn]
        (case (:f op)
          :read (assoc op :type :ok, :value (read-accounts t))

          :transfer (let [{:keys [from to amount]} (:value op)
                          [from to] (disorderly
                                      (find-account t from)
                                      (find-account t to))
                          _ (info :from (pr-str from))
                          _ (info :to   (pr-str to))
                          from' (update from :amount - amount)
                          to'   (update to   :amount + amount)]
                      (disorderly
                        (write-account! t from')
                        (write-account! t to'))
                      (if (neg? (:amount from'))
                        ; Whoops! Back out! Hey let's write some garbage just
                        ; to make things fun.
                        (do (write-account! t (update from' :amount - 1000))
                            (write-account! t (update to'   :amount - 1000))
                            (c/abort-txn! t)
                           (assoc op :type :fail, :error :insufficient-funds))
                        (assoc op :type :ok)))))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn workload
  "Stuff you need to build a test!"
  [opts]
  (merge (bank/test)
         {:client (Client. nil)}))