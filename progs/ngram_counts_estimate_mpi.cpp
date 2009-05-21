

void estimate_discounts(ngram_type& ngram, discount_set_type& discounts)
{
  typedef std::map<count_type, count_type, std::less<count_type>, std::allocator<std::pair<const count_type, count_type> > > count_set_type;
  typedef std::vector<count_set_type, std::allocator<count_set_type> > count_map_type;
  
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  count_map_type count_of_counts(ngram.index.order() + 1);
  
  for (int order = (ngram_rank == 0 ? 1 : 2); order <= ngram.index.order(); ++ order) {
    const size_type pos_first = ngram.index[shard].offsets[order - 1];
    const size_type pos_last  = ngram.index[shard].offsets[order];
    
    for (size_type pos = pos_first; pos != pos_last; ++ pos) {
      const count_type count = ngram.counts[shard][pos];
      if (count)
	++ count_of_counts[order][count];
    }
  }
  
  count_map_type count_of_counts_map(count_of_counts);
  for (int rank = 0; rank < mpi_size; ++ rank) {
    if (rank == mpi_rank) {
      boost::iostreams::filtering_ostream stream;
      stream.push(boost::iostreams::gzip_compressor());
      stream.push(utils::mpi_device_bcast_sink(rank, 1024 * 1024));
      
      for (int order = 1; order < count_of_counts_map.size(); ++ order) {
	count_set_type::const_iterator citer_end = count_of_counts_map[order].end();
	for (count_set_type::const_iterator citer = count_of_counts_map[order].begin(); citer != citer_end; ++ citer)
	  stream << order << '\t' << citer->first << ' ' << citer->second << '\n';
      }
    } else {
      boost::iostreams::filtering_istream stream;
      stream.push(boost::iostreams::gzip_decompressor());
      stream.push(utils::mpi_device_bcast_source(rank, 1024 * 1024));
      
      std::string line;
      tokens_type tokens;
      
      while (utils::readline(stream, line)) {
	tokenizer_type tokenizer(line);
	tokens.clear();
	tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
	
	if (tokens.size() != 3) continue;
	
	const int order = atoi(tokens[0].c_str());
	const count_type count = atoll(tokens[1].c_str());
	const count_type count_count = atoll(tokens[2].c_str());
	
	count_of_counts[order][count] += count_count;
      }
    }
  }
  
  discounts.clear();
  discounts.resize(ngram.index.order() + 1);
  
  for (int order = 1; order <= ngram.index.order(); ++ order)
    discounts[order].estimate(count_of_counts[order].begin(), count_of_counts[order].end());
  
  if (debug && mpi_rank == 0)
    for (int order = 1; order <= index.order(); ++ order)
      std::cerr << "order: " << order << ' ' << discounts[order] << std::endl;
}


void estimate_unigram(ngram_type& ngram, shard_data& shard, const discount_set_type& discounts, const bool remove_unk)
{
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  const id_type bos_id = ngram.index.vocab()[vocab_type::BOS];
  const id_type unk_id = ngram.index.vocab()[vocab_type::UNK];
  
  // we do not perform clipping by offsets for efficiency...
  const size_type logprob_size = ngram.index[mpi_rank].size();
  const size_type backoff_size = ngram.index[mpi_rank].position_size();
  
  shard.logprobs.reserve(logprob_size);
  shard.backoffs.reserve(backoff_size);
  
  shard.logprobs.resize(logprob_size, shard.logprob_min);
  shard.backoffs.resize(backoff_size, 0.0);
  
  // estimte only by root rank
  if (mpi_rank == 0) {
    if (debug)
      std::cerr << "order: "  << 1 << std::endl;
    
    shard.smooth = boost::numeric::bounds<logprob_type>::lowest();
    
    count_type total = 0;
    count_type observed = 0;
    count_type min2 = 0;
    count_type min3 = 0;
    count_type zero_events = 0;
    
    for (size_type pos = 0; pos < ngram.index[0].offsets[1]; ++ pos) {
      if (id_type(pos) == bos_id) continue;
      
      const count_type count = ngram.counts[0][pos];
      
      // when remove-unk is enabled, we treat it as zero_events
      if (count == 0 || (remove_unk && id_type(pos) == unk_id)) {
	++ zero_events;
	continue;
      }
      
      total += count;
      ++ observed;
      min2 += (count >= discounts[1].mincount2);
      min3 += (count >= discounts[1].mincount3);
    }
    
    double logsum = 0.0;
    const prob_type uniform_distribution = 1.0 / observed;
    
    for (/**/; logsum >= 0.0; ++ total) {
      logsum = boost::numeric::bounds<double>::lowest();
	
      for (size_type pos = 0; pos < ngram.index[0].offsets[1]; ++ pos) {
	if (id_type(pos) == bos_id) continue;
	  
	const count_type count = ngram.counts[0][pos];
	  
	if (count == 0 || (remove_unk && id_type(pos) == unk_id)) continue;
	  
	const prob_type discount = discounts[1].discount(count, total, observed);
	const prob_type prob = (discount * count / total);
	const prob_type lower_order_weight = discounts[1].lower_order_weight(total, observed, min2, min3);
	const prob_type lower_order_prob = uniform_distribution;
	const prob_type prob_combined = prob + lower_order_weight * lower_order_prob;
	const logprob_type logprob = utils::mathop::log(prob_combined);
	
	shard.logprobs[pos] = logprob;
	if (id_type(pos) == unk_id)
	  shard.smooth = logprob;
	  
	logsum = utils::mathop::logsum(logsum, double(logprob));
      }
    }
    
    const double discounted_mass =  1.0 - utils::mathop::exp(logsum);
    if (discounted_mass > 0.0) {
      if (zero_events > 0) {
	// distribute probability mass to zero events...
	  
	// if we set remove_unk, then zero events will be incremented when we actually observed UNK
	  
	const double logdistribute = utils::mathop::log(discounted_mass) - utils::mathop::log(zero_events);
	for (size_type pos = 0; pos < ngram.index[0].offsets[1]; ++ pos)
	  if (id_type(pos) != bos_id && (ngram.counts[0][pos] == 0 || (remove_unk && id_type(pos) == unk_id)))
	    shard.logprobs[pos] = logdistribute;
	if (ngram.smooth == boost::numeric::bounds<logprob_type>::lowest())
	  ngram.smooth = logdistribute;
      } else {
	for (size_type pos = 0; pos < ngram.index[0].offsets[1]; ++ pos)
	  if (id_type(pos) != bos_id) {
	    shard.logprobs[pos] -= logsum;
	    if (id_type(pos) == unk_id)
	      ngram.smooth = shard.logprobs[pos];
	  }
      }
    }
    
    // fallback to uniform distribution...
    if (shard.smooth == boost::numeric::bounds<logprob_type>::lowest())
      shard.smooth = utils::mathop::log(uniform_distribution);
    
    if (debug)
      std::cerr << "\tsmooth: " << shard.smooth << std::endl;
  }
  
  // disribute from root
  const int unigram_size = ngram.index[mpi_rank].offsets[1];
  MPI::COMM_WORLD.Bcast(&(*shard.logprobs.begin()), unigram_size, MPI::FLOAT, 0);
  MPI::COMM_WORLD.Bcast(&shard.smooth, 1, MPI::FLOAT, 0);
}


void estimate_bigram_mapper(intercomm_type& estimator, ngram_type& ngram, const bool remove_unk)
{
  typedef boost::iostreams::filtering_ostream ostream_type;
  typedef utils::mpi_device_sink              odevice_type;
  
  typedef boost::shared_ptr<ostream_type> ostream_ptr_type;
  typedef boost::shared_ptr<odevice_type> odevice_ptr_type;
  
  typedef std::vector<ostream_ptr_type, std::allocator<ostream_ptr_type> > ostream_ptr_set_type;
  typedef std::vector<odevice_ptr_type, std::allocator<odevice_ptr_type> > odevice_ptr_set_type;

  typedef std::vector<id_type, std::allocator<id_type> > context_type;
  
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  ostream_ptr_set_type stream(mpi_size);
  odevice_ptr_set_type device(mpi_size);
  
  for (int rank = 0; rank < mpi_size; ++ rank) {
    stream[rank].reset(new ostream_type());
    device[rank].reset(new odevice_type(estimator.comm, rank, bigram_mapper_tag, 1024 * 1024, false, true));
    
    stream[rank]->push(boost::iostreams::gzip_compressor());
    stream[rank]->push(*device[rank]);
  }
  
  const id_type bos_id = ngram.index.vocab()[vocab_type::BOS];
  const id_type unk_id = ngram.index.vocab()[vocab_type::UNK];
  
  const int order_prev = 1;
  const int order = 2;
  
  context_type   context(order);
  
  const size_type pos_context_first = ngram.index[mpi_rank].offsets[order_prev - 1];
  const size_type pos_context_last  = ngram.index[mpi_rank].offsets[order_prev];
  
  if (debug)
    std::cerr << "rank: " << mpi_rank << " order: " << order << std::endl;
  
  size_type pos_last_prev = pos_context_last;
  for (size_type pos_context = pos_context_first; pos_context < pos_context_last; ++ pos_context) {
    const size_type pos_first = pos_last_prev;
    const size_type pos_last = ngram.index[mpi_rank].children_last(pos_context);
    pos_last_prev = pos_last;
    
    if (pos_first == pos_last) continue;
    
    context.front() = id_type(pos_context);
    ostream_type& ostream = *stream[pos_context % mpi_size];
    
    for (size_type pos = pos_first; pos != pos_last; ++ pos) {
      context.back() = ngram.index[mpi_rank][pos];
      const count_type count = ngram.counts[mpi_rank][pos];
      
      if (count == 0 || (remove_unk && context.back() == unk_id)) continue;
      
      std::copy(context.begin(), context.end(), std::ostream_iterator<id_type>(ostream, " "));
      ostream << count << '\n';
    }
    
    if (mpi_flush_devices(stream, device))
      boost::thread::yield();
  }
  
  for (int rank = 0; rank < mpi_size; ++ rank) {
    *stream[rank] << '\n';
    stream[rank].reset();
  }
  
  // loop-until terminated...
  for (;;) {
    if (std::count(device.begin(), device.end(), odevice_ptr_type()) == mpi_size) break;
    
    if (! utils::mpi_terminate_devices(stream, device))
      boost::thread::yield();
  }
}

void estimate_bigram_estimator(intercomm_type& mapper, intercomm_type& reducer, ngram_type& ngram, const discount_set_type& discounts)
{
  typedef boost::iostreams::filtering_istream istream_type;
  typedef boost::iostreams::filtering_ostream ostream_type;
  
  typedef utils::mpi_device_source            idevice_type;
  typedef utils::mpi_device_sink              odevice_type;
  
  typedef boost::shared_ptr<istream_type> istream_ptr_type;
  typedef boost::shared_ptr<ostream_type> ostream_ptr_type;
  
  typedef boost::shared_ptr<idevice_type> idevice_ptr_type;
  typedef boost::shared_ptr<odevice_type> odevice_ptr_type;
  
  typedef std::vector<istream_ptr_type, std::allocator<istream_ptr_type> > istream_ptr_set_type;
  typedef std::vector<ostream_ptr_type, std::allocator<ostream_ptr_type> > ostream_ptr_set_type;
  
  typedef std::vector<idevice_ptr_type, std::allocator<idevice_ptr_type> > idevice_ptr_set_type;
  typedef std::vector<odevice_ptr_type, std::allocator<odevice_ptr_type> > odevice_ptr_set_type;
  
  typedef std::vector<id_type, std::allocator<id_type> > context_type;

  // we perform actual estimation
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  istream_ptr_set_type istream(mpi_size);
  ostream_ptr_set_type ostream(mpi_size);
  
  idevice_ptr_set_type idevice(mpi_size);
  odevice_ptr_set_type odevice(mpi_size);
  
  for (int rank = 0; rank < mpi_size; ++ rank) {
    istream[rank].reset(new istream_type());
    ostream[rank].reset(new ostream_type());
    
    idevice[rank].reset(new idevice_type(mapper.comm,  rank, bigram_mapper_tag, 1024 * 1024));
    odevice[rank].reset(new odevice_type(reducer.comm, rank, bigram_reducer_tag, 1024 * 1024, false, true));
    
    istream[rank]->push(boost::iostreams::gzip_decompressor());
    ostream[rank]->push(boost::iostreams::gzip_compressor());
    
    istream[rank]->push(*idevice[rank]);
    ostream[rank]->push(*odevice[rank]);
  }
  
  
}

void estimate_bigram_reducer(intercomm_type& estimator, ngram_type& ngram, shard_data& shard, const discount_set_type& discounts)
{
  // final reduction into shards...
  
  
}


void estimate_bigram(ngram_type& ngram, shard_data& shard, const discount_set_type& discounts, const bool remove_unk)
{
  // since bigrams are indexed wrt bigram hash, we need to map-reduce by unigram-hash for bigram probability estimates.
  
  
  
}

template <typename Tp>
class Event
{
public:
  typedef Tp        value_type;
  typedef size_t    size_type;
  typedef ptrdiff_t difference_type;

  typedef const Tp& const_reference;
  typedef       Tp&       reference;
  
private:
  struct impl;

public:
  Event() : pimpl(new impl()) {}
  Event(const Tp& x) : pimpl(new impl(x)) {}
  
  operator const_reference() const { return pimpl->value; }
  operator       reference()       { return pimpl->value; }
  
  
private:
  boost::shared_ptr<impl> pimpl;
};


struct EstimateLowerMapper
{
  
  void operator()()
  {
    const id_type bos_id = ngram.index.vocab()[vocab_type::BOS];
    const id_type unk_id = ngram.index.vocab()[vocab_type::UNK];
    
    const int order_max = ngram.index.order();
    
    context_type     context;
    logprob_set_type lowers;
    
    for (int order_prev = 2; order_prev < order_max; ++ order_prev) {
      const int order = order_prev + 1;

      if (debug)
	std::cerr << "order: " << order << " shard: " << shard << std::endl;
      
      const size_type pos_context_first = ngram.index[shard].offsets[order_prev - 1];
      const size_type pos_context_last  = ngram.index[shard].offsets[order_prev];
      
      context.resize(order);
      
      size_type pos_last_prev = pos_context_last;
      for (size_type pos_context = pos_context_first; pos_context < pos_context_last; ++ pos_context) {
	const size_type pos_first = pos_last_prev;
	const size_type pos_last = ngram.index[shard].children_last(pos_context);
	pos_last_prev = pos_last;
	
	if (pos_first == pos_last) continue;
	
	context_type::iterator citer_curr = context.end() - 2;
	for (size_type pos_curr = pos_context; pos_curr != size_type(-1); pos_curr = ngram.index[shard].parent(pos_curr), -- citer_curr)
	  *citer_curr = ngram.index[shard][pos_curr];
	
	lowers.clear();
	lowers.resize(pos_last - pos_first);

	count_type total = 0;
	
	logprob_set_type::iterator liter = lowers.begin();
	for (size_type pos = pos_first; pos != pos_last; ++ pos, ++ liter) {
	  const count_type count = ngram.counts[shard][pos];
	  
	  if (remove_unk && ngram.index[shard][pos] == unk_id) continue;
	  if (count == 0) continue;

	  total += count;
	  
	  context.back() = ngram.index[shard][pos];
	  
	  const int shard_index = ngram.index.shard_index(context.begin() + 1, context.end());
	  queue_ngram[shard_index]->push(std::make_pair(context_type(context.begin() + 1, context.end()), *liter));
	}
	
	if (total == 0) continue;
	
	queue_lowers.push_swap(lowers);
      }
    }
  }  
};

struct EstimateMapper
{
  void operator()()
  {
    const id_type bos_id = ngram.index.vocab()[vocab_type::BOS];
    const id_type unk_id = ngram.index.vocab()[vocab_type::UNK];
    
    const int order_max = ngram.index.order();
    
    context_type     context;
    logprob_set_type lowers;
    
    for (int order_prev = 2; order_prev < order_max; ++ order_prev) {
      const int order = order_prev + 1;

      if (debug)
	std::cerr << "order: " << order << " shard: " << shard << std::endl;
      
      const size_type pos_context_first = ngram.index[shard].offsets[order_prev - 1];
      const size_type pos_context_last  = ngram.index[shard].offsets[order_prev];
      
      context.resize(order);
      
      size_type pos_last_prev = pos_context_last;
      for (size_type pos_context = pos_context_first; pos_context < pos_context_last; ++ pos_context) {
	const size_type pos_first = pos_last_prev;
	const size_type pos_last = ngram.index[shard].children_last(pos_context);
	pos_last_prev = pos_last;
	
	if (pos_first == pos_last) continue;
	
	context_type::iterator citer_curr = context.end() - 2;
	for (size_type pos_curr = pos_context; pos_curr != size_type(-1); pos_curr = ngram.index[shard].parent(pos_curr), -- citer_curr)
	  *citer_curr = ngram.index[shard][pos_curr];
	
	count_type total = 0;
	count_type observed = 0;
	count_type min2 = 0;
	count_type min3 = 0;
	count_type zero_events = 0;
	
	// lowers!
	queue_lowers.pop_swap(lowers);
	
	logprob_set_type::iterator liter = lowers.begin();
	for (size_type pos = pos_first; pos != pos_last; ++ pos, ++ liter) {
	  const count_type count = ngram.counts[shard][pos];
	  
	  if (remove_unk && ngram.index[shard][pos] == unk_id) continue;
	  
	  if (count == 0) {
	    ++ zero_events;
	    continue;
	  }
	  
	  total += count;
	  ++ observed;
	  min2 += (count >= discounts[order].mincount2);
	  min3 += (count >= discounts[order].mincount3);
	  
	  liter->wait();
	  logsum_lower_order = utils::mathop::logsum(logsum_lower_order, double(*liter));
	}
	
	if (total == 0) continue;
	
	
	
      }
    }
  }
};


struct EstimateReducer
{
  
  void operator()()
  {
    while (1) {
      queue.pop_swap(context_parameter);
      
      if (context_parameter.first.empty()) break;
      
      const context_type& context = context_parameter.fist;
      
      std::pair<context_type::const_iterator, size_type> result = ngram.index.traverse(shard_index, context.begin(), context.end());
      if (result.second != contex.end() || result.second == size_type(-1))
	throw std::runtime_error("no ngram?");
      
      if (context_parameter.second.first != logprob_min())
	shard.logprobs[result.second] = context_parameter.second.first;
      if (context_parameter.second.second != logprob_min())
	shard.backoffs[result.second] = context_parameter.second.second;
      
      size_type position;
      size_type position_new;
      do {
	utils::atomicop::memory_barrier();
	position = logprob_offset;
	positions_new = std::max(positions, result.second);
      } while (! utils::atomicop::compare_and_swap(logprob_offset, position, position_new));
    }
  }
  
};

// ngram query...
struct EstimateNGram
{
  
  
  template <typename ContextLogprob, typename Pending, typename Pendings>
  void logprob_ngram(ContextLogprob& context_logprob, Pending& pending, Pendings& pendings)
  {
    context_type::const_iterator first = context_logprob.first.begin();
    context_type::const_iterator last  = context_logprob.first.end();
    
    for (/**/; first != last - 1; ++ first) {
      const size_type shard_index = ngram.index.shard_index(first, last);
      
      if (shard_index == mpi_rank) {
	std::pair<context_type::const_iterator, size_type> result = ngram.index.traverse(shard_index, first, last);
	  
	if (result.first == last) {
	  
	  // do we stop here...?
	  if (utils::atomicop::fetch_and_add(logprob_offset, size_type(0)) <= result.second) {
	    pending.push_back(std::make_pair(result.second, ContextLogprob(context_type(first, last), context_logprob.second)));
	    break;
	  }
	  
	  if (shard.logprobs[result.second] != logprob_min) {
	    context_logprob.second += shard.logprobs[result.second];
	    context_logprob.second.notify(); // notify here!
	    break;
	  } else {
	    const size_type parent = ngram.index[shard_index].parent(result.second);
	    if (parent != size_type(-1))
	      context_logprob.second += shard.logprobs[parent];
	  }
	    
	} else if (result.first == last - 1)
	  context_logprob.second += shard.backoffs[result.second];
	  
      } else {
	// send...
	ContextLogprob context_logprob_backoff(context_type(first, last), context_logprob.second);
	if (! queues[shard_index]->push_swap(context_logprob_backoff, true))
	  pendings[shard_index].push_back(context_logprob_backoff);
	break;
      }
    }
    
    // unigram... we do not have to backoff...
    context_logprobs.second += shard.logprobs[*first];
    context_logprobs.second.notify();
  }


  void operator()()
  {
    context_logprob_type context_logprob;

    while (1) {
      bool found = false;
      
      for (int rank = 0; rank < pendings.size(); ++ rank)
	while (! pendings[rank].empty() && queues[rank]->push_swap(pengings[rank].front(), true)) {
	  pendings[rank].pop_front();
	  found = true;
	}
      
      while (! pending.empty() && pending.front().first < utils::atomicop::fetch_and_add(logprob_offset, size_type(0))) {
	logprob_ngram(pending.front().second, pending, pendings);
	pending.pop_front();
	found = true;
      }
      
      if (! queue.pop(context_logprob, true)) {
	
	// do we break here?
	if (context_logprob.first.empty()) break;
	
	logprob_ngram(context_logprob, pending, pendings);
	
	found = true;
      }
      
      if (! found)
	boost::thread::yield();
    }
    
    // process all the pending events...??
  }
};

// here, we perform estimation, and final  result sent to reducer
// we will also query logprobabiliteis...
void estimate_mapper(intercomm_type& reducer, ngram_type& ngram, const discount_set_type& discounts, const bool remove_unk)
{
  
  
}

// here, we perform probability updates and also perform scoring..
void estimate_reducer(intercomm_type& mapper, ngram_type& ngram, shard_data& shard)
{
  // here, we maintain two channels
  // 1. probability/backoff update stream (input stream only)
  //    
  // 2. probability query stream (input and output streams)
  //    we may subsequently query other shards...
  //
  
  
}
