/*
 * Copyright 2022-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jpa.repository.query;

import com.mysema.commons.lang.Assert;
import org.jspecify.annotations.Nullable;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * The implementation of the Regex-based {@link QueryEnhancer} using {@link QueryUtils}.
 *
 * @author Diego Krupitza
 * @since 2.7.0
 */
class DefaultQueryEnhancer implements QueryEnhancer {

	private final QueryProvider query;
	private final boolean hasConstructorExpression;
	private final @Nullable  String alias;
	private final String projection;

	public DefaultQueryEnhancer(QueryProvider query) {
		this.query = query;
		this.hasConstructorExpression = QueryUtils.hasConstructorExpression(query.getQueryString());
		this.alias = QueryUtils.detectAlias(query.getQueryString());
		this.projection = QueryUtils.getProjection(this.query.getQueryString());
	}

	@Override
	public String rewrite(QueryRewriteInformation rewriteInformation) {
		String queryString = this.query.getQueryString();

		if (!isSelectQuery(queryString)) {
			throw new UnsupportedOperationException(
					"Cannot apply sorting to non-SELECT queries. Query type not supported for sorting operations");
		}

		return QueryUtils.applySorting(this.query.getQueryString(), rewriteInformation.getSort(), alias);
	}

	@Override
	public String createCountQueryFor(@Nullable String countProjection) {
		String queryString = this.query.getQueryString();

		if (!isSelectQuery(queryString)) {
			throw new UnsupportedOperationException(
					"Cannot create count query for non-SELECT queries. Query type not supported for count operations");
		}

		boolean nativeQuery = this.query instanceof DeclaredQuery dc ? dc.isNative() : true;
		return QueryUtils.createCountQueryFor(this.query.getQueryString(), countProjection, nativeQuery);
	}

	/**
	 * Determines whether the given query is a SELECT-based query.
	 * Only SELECT and WITH queries are considered valid for sorting and count operations.
	 *
	 * @param queryString the query string to check
	 * @return {@literal true} if the query is a SELECT-based query, {@literal false} otherwise
	 */
	private boolean isSelectQuery(String queryString) {

		Assert.hasText(queryString, "Query string must not be null or empty");

		String trimmedQuery = queryString.trim().toLowerCase(Locale.ENGLISH);

		if (trimmedQuery.startsWith("delete") ||
				trimmedQuery.startsWith("update") ||
				trimmedQuery.startsWith("insert") ||
				trimmedQuery.startsWith("create") ||
				trimmedQuery.startsWith("drop") ||
				trimmedQuery.startsWith("alter") ||
				trimmedQuery.startsWith("truncate")) {
			return false;
		}

		Pattern aliasPattern = Pattern.compile("^from\\s+\\w+\\s+([a-zA-Z]\\w*)(?=\\s|$|\\W)");
		//Pattern aliasPattern = Pattern.compile("^from\\s+\\w+\\s+\\w+(?=\\s|$)", Pattern.CASE_INSENSITIVE);

		return trimmedQuery.startsWith("select") || trimmedQuery.startsWith("with") || aliasPattern.matcher(trimmedQuery).find();
	}

	@Override
	public boolean hasConstructorExpression() {
		return this.hasConstructorExpression;
	}

	@Override
	public @Nullable String detectAlias() {
		return this.alias;
	}

	@Override
	public String getProjection() {
		return this.projection;
	}

	@Override
	public QueryProvider getQuery() {
		return this.query;
	}

}
