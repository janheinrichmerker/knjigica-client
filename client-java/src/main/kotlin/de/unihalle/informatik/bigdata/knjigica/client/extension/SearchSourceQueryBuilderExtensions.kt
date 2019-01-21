package de.unihalle.informatik.bigdata.knjigica.client.extension

import org.elasticsearch.index.query.*
import org.elasticsearch.index.query.QueryBuilders.*
import org.elasticsearch.search.builder.SearchSourceBuilder

inline fun SearchSourceBuilder.queryBool(block: BoolQueryBuilder.() -> Unit): SearchSourceBuilder =
        query(bool(block))

inline fun bool(block: BoolQueryBuilder.() -> Unit): BoolQueryBuilder =
        boolQuery().apply(block)

fun BoolQueryBuilder.must(vararg queries: QueryBuilder): BoolQueryBuilder =
        queries.fold(this) { builder, query -> builder.must(query) }


fun BoolQueryBuilder.should(vararg queries: QueryBuilder): BoolQueryBuilder =
        queries.fold(this) { builder, query -> builder.should(query) }

infix fun String.match(text: Any): MatchQueryBuilder =
        matchQuery(this, text)

inline fun String.match(text: Any, block: MatchQueryBuilder.() -> Unit): MatchQueryBuilder =
        match(text).apply(block)


