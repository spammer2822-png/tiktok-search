/* Embedded in the offline HTML. One row store, index-only filters/sorts.
   Runs in a Blob Worker; the cooperative fallback also works without Workers. */
function createReportEngine(post, cooperative) {
  'use strict';
  let rows = [], searches = [], filtered = [], latest = 0, cacheFilter = '', cacheSort = '';
  const collator = new Intl.Collator(undefined, {numeric: true, sensitivity: 'base'});
  const pause = () => new Promise(resolve => setTimeout(resolve, 0));
  const view = (r, key) => {
    switch (key) {
      case 'matches': return r.target_found;
      case 'mutuals': return r.mutual === true;
      case 'skipped': return r.skipped;
      case 'errors': return r.hard_error || r.status === 'partial';
      case 'complete': return r.complete;
      case 'partial': return r.status === 'partial';
      case 'private': return r.status === 'private';
      default: return true;
    }
  };
  async function initialize(message) {
    let text = message.text;
    const metadata = message.metadata, fields = metadata.row_fields;
    const keys = ['status','phase','relationship_label','skip_reason'];
    const sets = Object.fromEntries(keys.map(k => [k,new Set()]));
    let position = 0, end;
    while ((end = text.indexOf('\n', position)) !== -1) {
      if (end > position) {
        const values = JSON.parse(text.slice(position, end));
        const delta = metadata.row_encoding === 'indexed_delta_v1';
        const r = delta ? {...metadata.row_defaults} : {};
        if (delta) {
          for(let i=0;i<values.length;i+=2)r[fields[values[i]]]=values[i+1];
        } else {
          for(let i=0;i<fields.length;i++)r[fields[i]]=values[i];
        }
        r._id = rows.length;
        rows.push(r);
        searches.push([r.username,r.display_name,r.uid,r.status,r.phase,r.relationship_label,
          r.sources.join(' '),r.skip_reason,r.error,r.found_in.join(' ')].join(' ').toLowerCase());
        for (const key of keys) {
          const set=sets[key];
          // Arbitrarily many unique error messages must not create a huge select DOM.
          // The reason filter is editable, with up to 200 suggestions, not restricted to them.
          if (key !== 'skip_reason' || set.size < 200) set.add(String(r[key] ?? 'unknown'));
        }
        if (cooperative && rows.length % 500 === 0) await pause();
      }
      position = end + 1;
    }
    text = message.text = '';
    const values = Object.fromEntries(Object.entries(sets).map(([k,set])=>[k,[...set].filter(Boolean).sort(collator.compare)]));
    post({type:'ready', metadata, values, total:rows.length, fallback:cooperative});
  }
  function comparison(key, ascending) {
    return (i,j) => {
      const a=rows[i], b=rows[j], av=a[key], bv=b[key];
      if (av === null || av === undefined) return bv === null || bv === undefined ? i-j : 1;
      if (bv === null || bv === undefined) return -1;
      const n=typeof av==='number' && typeof bv==='number' ? av-bv : typeof av==='boolean' ? Number(av)-Number(bv) : collator.compare(String(av),String(bv));
      return (ascending ? n : -n) || collator.compare(a.username,b.username) || i-j;
    };
  }
  async function cooperativeSort(indices, compare, token) {
    // Native sort is safe in the Worker. A blocked-Worker browser gets chunked
    // bottom-up merge sort, yielding between bounded batches on the UI thread.
    if (!cooperative) return indices.sort(compare);
    let src=indices, dst=new Array(indices.length), steps=0;
    for(let width=1;width<src.length;width*=2) {
      for(let start=0;start<src.length;start+=width*2) {
        let i=start,j=Math.min(start+width,src.length), mid=j,end=Math.min(start+width*2,src.length),k=start;
        while(i<mid || j<end) {
          dst[k++]=j>=end || (i<mid && compare(src[i],src[j])<=0) ? src[i++] : src[j++];
          if(++steps%2048===0) {await pause();if(token!==latest)return null;}
        }
      }
      [src,dst]=[dst,src];
    }
    return src;
  }
  async function query(message) {
    const token=++latest, q=message.query;
    const key=JSON.stringify([q.term,q.section,q.quick,q.active]), order=JSON.stringify([q.sort,q.ascending]);
    if(key!==cacheFilter || order!==cacheSort) {
      let indices=key===cacheFilter ? filtered.slice() : [];
      if(key!==cacheFilter) {
      for(let i=0;i<rows.length;i++) {
        const r=rows[i];
        if(view(r,q.section) && view(r,q.quick) && (!q.term || searches[i].includes(q.term)) &&
          q.active.every(([k,v]) => k==='skip_reason' ? r.skip_reason.toLowerCase().includes(v.toLowerCase()) : String(r[k]===null?'unknown':r[k])===v)) indices.push(i);
        if(cooperative && i%4096===0) {await pause();if(token!==latest)return;}
      }
      }
      indices=await cooperativeSort(indices,comparison(q.sort,q.ascending),token);
      if(token!==latest || !indices)return;
      filtered=indices;cacheFilter=key;cacheSort=order;
    }
    const size=Math.max(1,Math.min(500,q.pageSize)), page=Math.max(1,Math.min(q.page,Math.ceil(filtered.length/size)||1));
    post({type:'page',requestId:message.requestId,page,total:filtered.length,
      rows:filtered.slice((page-1)*size,page*size).map(i=>rows[i])});
  }
  return async message => {
    try {
      if(message.type==='init') await initialize(message);
      if(message.type==='query') await query(message);
    } catch (_) {post({type:'error',message:'Unable to read this report. Saved scan JSON and checkpoints are unchanged.'});}
  };
}
