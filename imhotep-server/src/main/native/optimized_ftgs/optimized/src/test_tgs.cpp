#define restrict __restrict__
extern "C" {
#include "circ_buf.h" 
#include "imhotep_native.h"
#include "local_session.h"
}

#include "test_utils.h"

#include <algorithm>
#include <array>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <set>
#include <vector>

using namespace std;

typedef int     DocId;
typedef int64_t GroupId;
typedef int64_t Metric;

typedef vector<int>  DocIds;
typedef set<int64_t> GroupIds;

template <size_t n_metrics>
struct Metrics : public array<Metric, n_metrics>
{
  Metrics() { fill(this->begin(), this->end(), 0); } // !@# redundant?
};

template <size_t n_metrics>
struct Entry {
  typedef Metrics<n_metrics> Metrics;
  DocId   doc_id;
  Metrics metrics;
  GroupId group_id;

  Entry(DocId doc_id_, GroupId group_id_)
    : doc_id(doc_id_)
    , group_id(group_id_) {
    fill(metrics.begin(), metrics.end(), 0);
  }
};

typedef function<int64_t(size_t index)> DocIdFunc;
typedef function<int64_t(size_t doc_id)> GroupIdFunc;
typedef function<int64_t(int64_t min, int64_t max)> MetricFunc;

template <size_t n_metrics>
class Table : public vector<Entry<n_metrics>> {

  typedef Metrics<n_metrics>       Metrics;
  typedef Entry<n_metrics>         Entry;
  typedef multimap<GroupId, Entry> EntriesByGroup;

  const Metrics _mins;
  const Metrics _maxes;

 public:
  Table(size_t             n_docs,
        const Metrics&     mins,
        const Metrics&     maxes,
        const DocIdFunc&   doc_id_func,
        const GroupIdFunc& group_id_func,
        const MetricFunc&  metric_func) 
    : _mins(mins)
    , _maxes(maxes) {
    for (size_t doc_index(0); doc_index < n_docs; ++doc_index) {
      DocId   doc_id(doc_id_func(doc_index));
      GroupId group_id(group_id_func(doc_id));
      Entry   entry(doc_id, group_id);
      for (size_t metric_index(0); metric_index < n_metrics; ++metric_index) {
        entry.metrics[metric_index] = metric_func(mins[metric_index], maxes[metric_index]);
      }
      this->push_back(entry);
    }
  }

  Metrics mins()  const { return _mins;  }
  Metrics maxes() const { return _maxes; }

  DocIds doc_ids() const {
    DocIds result;
    transform(this->begin(), this->end(), back_inserter(result),
              [](const Entry& entry) { return entry.doc_id; });
    return result;
  }

  vector<GroupId> flat_group_ids() const {
    vector<GroupId> result;
    transform(this->begin(), this->end(), back_inserter(result),
              [](const Entry& entry) { return entry.group_id; });
    return result;
  }

  GroupIds group_ids() const {
    GroupIds result;
    transform(this->begin(), this->end(), inserter(result, result.begin()),
              [](const Entry& entry) { return entry.group_id; });
    return result;
  }

  EntriesByGroup entries_by_group() const {
    EntriesByGroup result;
    for (auto entry: *this) {
      result.insert(make_pair(entry.group_id, entry));
    }
    return result;
  }

  vector<Metric> metrics(size_t metric_index) const {
    vector<Metric> result;
    transform(this->begin(), this->end(), inserter(result, result.begin()),
              [metric_index, &result](const Entry& entry) { return entry.metrics[metric_index]; });
    return result;
  }

  vector<Metrics> metrics() const {
    vector<Metrics> result;
    for_each(this->begin(), this->end(),
             [&result](const Entry& entry) { result.push_back(entry.metrics); });
    return result;
  }

  Metrics sum(GroupId group_id) const {
    Metrics result;
    for (auto entry: *this) {
      if (entry.group_id == group_id) {
        for (size_t index(0); index < entry.metrics.size(); ++index) {
          result[index] += entry.metrics[index];
        }
      }
    }
    return result;
  }

  vector<Metrics> sum() const {
    vector<Metrics> result;
    const GroupIds       group_ids(this->group_ids());
    const EntriesByGroup entries(entries_by_group());
    for (auto group_id: group_ids) {
      Metrics row;
      auto range(entries.equal_range(group_id));
      for_each(range.first, range.second,
               [&row] (const typename EntriesByGroup::value_type& value) {
                 for (size_t metric_index(0); metric_index < n_metrics; ++metric_index) {
                   row[metric_index] += value.second.metrics[metric_index];
                 }
               });
      result.push_back(row);
    }
    return result;
  }
};

template <size_t n_metrics>
ostream& operator<<(ostream& os, const Metrics<n_metrics>& row) {
  for (auto element: row) {
    os << element << " ";
  }
  return os;
}

template <size_t n_metrics>
ostream& operator<<(ostream& os, const vector<Metrics<n_metrics>>& rows) {
  for (auto row: rows) {
    os << row << endl;
  }
  return os;
}

template <size_t n_metrics>
struct Shard {

  packed_shard_t *_shard;

  Shard(const Table<n_metrics>& table)
    : _shard(create_shard_multicache(table.size(), table.mins().data(), table.maxes().data(), n_metrics)) {

    DocIds          doc_ids(table.doc_ids());
    vector<GroupId> flat_group_ids(table.flat_group_ids());
    packed_shard_update_groups(_shard, doc_ids.data(), doc_ids.size(), flat_group_ids.data());

    for (size_t metric_index(0); metric_index < n_metrics; ++metric_index) {
      vector<Metric> metrics(table.metrics(metric_index));
      packed_shard_update_metric(_shard, doc_ids.data(), doc_ids.size(),
                                 metrics.data(), metric_index);
    }
  }

  ~Shard() { packed_shard_destroy(_shard); }

  packed_shard_t * operator()() { return _shard; };
};


template <typename int_t>
void dump(ostream& os, int_t value) {
  typedef array<uint8_t, sizeof(int_t)> byte_array;
  const byte_array& bytes(*reinterpret_cast<byte_array*>(&value));
  vector<uint8_t> byte_vec(bytes.begin(), bytes.end());
  os << byte_vec;
}

int main(int argc, char* argv[])
{
  constexpr size_t n_docs  = 32;
  constexpr size_t n_metrics = 10;
  constexpr size_t n_groups = 4;
  typedef Shard<n_metrics> TestShard;

  int status(EXIT_SUCCESS);

  Metrics<n_metrics> mins, maxes;
  fill(mins.begin(), mins.end(), 0);
  fill(maxes.begin(), maxes.end(), 13);
  // maxes[0] = 1;
  // maxes[1] = 1;
  // maxes[2] = 1;
  // maxes[3] = 1;

  Table<n_metrics> table(n_docs, mins, maxes, 
                         [](size_t index) { return index; },
                         [](size_t doc_id) { return doc_id % 4; }, // i.e. group_id == doc_id
                         [](int64_t min, int64_t max) { /*return (max - min) / 2;*/ return max; });

  cout << "table:" << endl;
  cout << table.metrics() << endl << endl;

  cout << "expected:" << endl;
  const vector<Metrics<n_metrics>> sum(table.sum());
  size_t sum_index(0);
  for (auto group_id: table.group_ids()) {
    cout << "gid " << group_id << ": ";
    cout << sum.at(sum_index) << endl;
    ++sum_index;
  }
  cout << endl;

  struct worker_desc worker;
  array <int, 1> socket_file_desc{{3}};
  worker_init(&worker, 1, n_groups, n_metrics, socket_file_desc.data(), 1);

  struct session_desc session;
  uint8_t shard_order[] = {0};
  session_init(&session, n_groups, n_metrics, shard_order, 1);

  array <int, 1> shard_handles;
  TestShard shard(table);
  shard_handles[0] = register_shard(&session, shard());

  DocIds doc_ids(table.doc_ids());
  vector<uint8_t> slice;
  doc_ids_encode(doc_ids.begin(), doc_ids.end(), slice);
  array<long, 1> addresses{{reinterpret_cast<long>(slice.data())}};

  array<int, 1> docs_in_term{{static_cast<int>(table.doc_ids().size())}};

  run_tgs_pass(&worker,
               &session,
               TERM_TYPE_INT,
               1,
               NULL,
               addresses.data(),
               docs_in_term.data(),
               shard_handles.data(),
               1,
               socket_file_desc[0]);

  cout << "actual:" << endl;
  GroupIds gids(table.group_ids());
  typedef array<uint64_t, n_metrics> Row;
  size_t row_index(0);
  for (GroupIds::const_iterator it(gids.begin()); it != gids.end(); ++it, ++row_index) {
    cout << "gid" << *it << ": ";
    const size_t vectors_per_row(n_metrics % 2 == 0 ? n_metrics / 2 : n_metrics / 2 + 1);
    const Row& row(*reinterpret_cast<Row*>(worker.group_stats_buf + vectors_per_row));
    cout << row << endl;
    ++row_index;
  }

  session_destroy(&session);
  // worker_destroy(&worker);

  return status;
}
