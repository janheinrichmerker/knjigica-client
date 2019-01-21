package de.unihalle.informatik.bigdata.knjigica.client.extension

import org.elasticsearch.action.search.SearchResponse

val SearchResponse.totalHits get() = hits.totalHits

fun SearchResponse.isEmpty() = totalHits <= 0
