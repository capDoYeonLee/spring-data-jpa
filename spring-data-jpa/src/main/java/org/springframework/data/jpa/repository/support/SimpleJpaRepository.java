/*
 * Copyright 2008-2025 the original author or authors.
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
package org.springframework.data.jpa.repository.support;

import static org.springframework.data.jpa.repository.query.QueryUtils.*;

import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Query;
import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaDelete;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.CriteriaUpdate;
import jakarta.persistence.criteria.Path;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;
import jakarta.persistence.criteria.Selection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.KeysetScrollPosition;
import org.springframework.data.domain.OffsetScrollPosition;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.convert.QueryByExamplePredicateBuilder;
import org.springframework.data.jpa.domain.DeleteSpecification;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.domain.UpdateSpecification;
import org.springframework.data.jpa.provider.PersistenceProvider;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.query.EscapeCharacter;
import org.springframework.data.jpa.repository.query.KeysetScrollDelegate;
import org.springframework.data.jpa.repository.query.KeysetScrollSpecification;
import org.springframework.data.jpa.repository.query.QueryUtils;
import org.springframework.data.jpa.repository.support.FetchableFluentQueryBySpecification.SpecificationScrollDelegate;
import org.springframework.data.jpa.repository.support.FluentQuerySupport.ScrollQueryFactory;
import org.springframework.data.jpa.repository.support.QueryHints.NoHints;
import org.springframework.data.jpa.support.PageableUtils;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.query.FluentQuery;
import org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.support.PageableExecutionUtils;
import org.springframework.data.util.Lazy;
import org.springframework.data.util.ProxyUtils;
import org.springframework.data.util.Streamable;
import org.springframework.lang.Contract;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

/**
 * Default implementation of the {@link org.springframework.data.repository.CrudRepository} interface. This will offer
 * you a more sophisticated interface than the plain {@link EntityManager} .
 *
 * @param <T> the type of the entity to handle
 * @param <ID> the type of the entity's identifier
 * @author Oliver Gierke
 * @author Eberhard Wolff
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Stefan Fussenegger
 * @author Jens Schauder
 * @author David Madden
 * @author Moritz Becker
 * @author Sander Krabbenborg
 * @author Jesse Wouters
 * @author Greg Turnquist
 * @author Yanming Zhou
 * @author Ernst-Jan van der Laan
 * @author Diego Krupitza
 * @author Seol-JY
 * @author Joshua Chen
 * @author Giheon Do
 */
@Repository
@Transactional(readOnly = true)
public class SimpleJpaRepository<T, ID> implements JpaRepositoryImplementation<T, ID> {

	private static final String ID_MUST_NOT_BE_NULL = "The given id must not be null";
	private static final String IDS_MUST_NOT_BE_NULL = "Ids must not be null";
	private static final String ENTITY_MUST_NOT_BE_NULL = "Entity must not be null";
	private static final String ENTITIES_MUST_NOT_BE_NULL = "Entities must not be null";
	private static final String EXAMPLE_MUST_NOT_BE_NULL = "Example must not be null";
	private static final String SPECIFICATION_MUST_NOT_BE_NULL = "Specification must not be null";
	private static final String QUERY_FUNCTION_MUST_NOT_BE_NULL = "Query function must not be null";

	private final JpaEntityInformation<T, ?> entityInformation;
	private final EntityManager entityManager;
	private final PersistenceProvider provider;

	private final Lazy<String> deleteAllQueryString;
	private final Lazy<String> countQueryString;

	private @Nullable CrudMethodMetadata metadata;
	private ProjectionFactory projectionFactory;
	private EscapeCharacter escapeCharacter = EscapeCharacter.DEFAULT;

	/**
	 * Creates a new {@link SimpleJpaRepository} to manage objects of the given {@link JpaEntityInformation}.
	 *
	 * @param entityInformation must not be {@literal null}.
	 * @param entityManager must not be {@literal null}.
	 */
	public SimpleJpaRepository(JpaEntityInformation<T, ?> entityInformation, EntityManager entityManager) {

		Assert.notNull(entityInformation, "JpaEntityInformation must not be null");
		Assert.notNull(entityManager, "EntityManager must not be null");

		this.entityInformation = entityInformation;
		this.entityManager = entityManager;
		this.provider = PersistenceProvider.fromEntityManager(entityManager);
		this.projectionFactory = new SpelAwareProxyProjectionFactory();

		this.deleteAllQueryString = Lazy
				.of(() -> getQueryString(DELETE_ALL_QUERY_STRING, entityInformation.getEntityName()));
		this.countQueryString = Lazy
				.of(() -> getQueryString(String.format(COUNT_QUERY_STRING, provider.getCountQueryPlaceholder(), "%s"),
						entityInformation.getEntityName()));
	}

	/**
	 * Creates a new {@link SimpleJpaRepository} to manage objects of the given domain type.
	 *
	 * @param domainClass must not be {@literal null}.
	 * @param entityManager must not be {@literal null}.
	 */
	public SimpleJpaRepository(Class<T> domainClass, EntityManager entityManager) {
		this(JpaEntityInformationSupport.getEntityInformation(domainClass, entityManager), entityManager);
	}

	/**
	 * Configures a custom {@link CrudMethodMetadata} to be used to detect {@link LockModeType}s and query hints to be
	 * applied to queries.
	 *
	 * @param metadata
	 */
	@Override
	public void setRepositoryMethodMetadata(CrudMethodMetadata metadata) {
		this.metadata = metadata;
	}

	@Override
	public void setEscapeCharacter(EscapeCharacter escapeCharacter) {
		this.escapeCharacter = escapeCharacter;
	}

	@Override
	public void setProjectionFactory(ProjectionFactory projectionFactory) {
		this.projectionFactory = projectionFactory;
	}

	protected @Nullable CrudMethodMetadata getRepositoryMethodMetadata() {
		return metadata;
	}

	protected Class<T> getDomainClass() {
		return entityInformation.getJavaType();
	}

	@Override
	@Transactional
	public void deleteById(ID id) {

		Assert.notNull(id, ID_MUST_NOT_BE_NULL);

		findById(id).ifPresent(this::delete);
	}

	@Override
	@Transactional
	@SuppressWarnings("unchecked")
	public void delete(T entity) {

		Assert.notNull(entity, ENTITY_MUST_NOT_BE_NULL);

		doDelete(entityManager, entityInformation, entity);
	}

	static <T> boolean doDelete(EntityManager entityManager, JpaEntityInformation<T, ?> entityInformation, T entity) {

		if (entityInformation.isNew(entity)) {
			return false;
		}

		if (entityManager.contains(entity)) {
			entityManager.remove(entity);
			return true;
		}

		Class<?> type = ProxyUtils.getUserClass(entity);

		// if the entity to be deleted doesn't exist, delete is a NOOP
		T existing = (T) entityManager.find(type, entityInformation.getId(entity));
		if (existing != null) {
			entityManager.remove(entityManager.merge(entity));

			return true;
		}

		return false;
	}

	@Override
	@Transactional
	public void deleteAllById(Iterable<? extends ID> ids) {

		Assert.notNull(ids, IDS_MUST_NOT_BE_NULL);

		for (ID id : ids) {
			deleteById(id);
		}
	}

	@Override
	@Transactional
	public void deleteAllByIdInBatch(Iterable<ID> ids) {

		Assert.notNull(ids, IDS_MUST_NOT_BE_NULL);

		if (!ids.iterator().hasNext()) {
			return;
		}

		if (entityInformation.hasCompositeId()) {

			List<T> entities = new ArrayList<>();
			// generate entity (proxies) without accessing the database.
			ids.forEach(id -> entities.add(getReferenceById(id)));
			deleteAllInBatch(entities);
		} else {

			String queryString = String.format(DELETE_ALL_QUERY_BY_ID_STRING, entityInformation.getEntityName(),
					entityInformation.getRequiredIdAttribute().getName());

			Query query = entityManager.createQuery(queryString);

			/*
			 * Some JPA providers require {@code ids} to be a {@link Collection} so we must convert if it's not already.
			 */
			Collection<ID> idCollection = toCollection(ids);
			query.setParameter("ids", idCollection);

			applyQueryHints(query);

			query.executeUpdate();
		}
	}

	@Override
	@Transactional
	public void deleteAll(Iterable<? extends T> entities) {

		Assert.notNull(entities, ENTITIES_MUST_NOT_BE_NULL);

		for (T entity : entities) {
			delete(entity);
		}
	}

	@Override
	@Transactional
	public void deleteAllInBatch(Iterable<T> entities) {

		Assert.notNull(entities, ENTITIES_MUST_NOT_BE_NULL);

		if (!entities.iterator().hasNext()) {
			return;
		}

		applyAndBind(getQueryString(DELETE_ALL_QUERY_STRING, entityInformation.getEntityName()), entities, entityManager)
				.executeUpdate();
	}

	@Override
	@Transactional
	public void deleteAll() {

		for (T element : findAll()) {
			delete(element);
		}
	}

	@Override
	@Transactional
	public void deleteAllInBatch() {

		Query query = entityManager.createQuery(deleteAllQueryString.get());

		applyQueryHints(query);

		query.executeUpdate();
	}

	@Override
	public Optional<T> findById(ID id) {

		Assert.notNull(id, ID_MUST_NOT_BE_NULL);

		Class<T> domainType = getDomainClass();

		if (metadata == null) {
			return Optional.ofNullable(entityManager.find(domainType, id));
		}

		LockModeType type = metadata.getLockModeType();
		Map<String, Object> hints = getHints();

		return Optional.ofNullable(
				type == null ? entityManager.find(domainType, id, hints) : entityManager.find(domainType, id, type, hints));
	}

	@Deprecated
	@Override
	public T getOne(ID id) {
		return getReferenceById(id);
	}

	@Deprecated
	@Override
	public T getById(ID id) {
		return getReferenceById(id);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jpa.repository.JpaRepository#getReferenceById(java.io.Serializable)
	 */
	@Override
	public T getReferenceById(ID id) {

		Assert.notNull(id, ID_MUST_NOT_BE_NULL);
		return entityManager.getReference(getDomainClass(), id);
	}

	@Override
	public boolean existsById(ID id) {

		Assert.notNull(id, ID_MUST_NOT_BE_NULL);

		if (entityInformation.getIdAttribute() == null) {
			return findById(id).isPresent();
		}

		String placeholder = provider.getCountQueryPlaceholder();
		String entityName = entityInformation.getEntityName();
		Iterable<String> idAttributeNames = entityInformation.getIdAttributeNames();
		String existsQuery = QueryUtils.getExistsQueryString(entityName, placeholder, idAttributeNames);

		TypedQuery<Long> query = entityManager.createQuery(existsQuery, Long.class);

		applyQueryHints(query);

		if (!entityInformation.hasCompositeId()) {
			query.setParameter(idAttributeNames.iterator().next(), id);
			return query.getSingleResult() == 1L;
		}

		for (String idAttributeName : idAttributeNames) {

			Object idAttributeValue = entityInformation.getCompositeIdAttributeValue(id, idAttributeName);

			boolean complexIdParameterValueDiscovered = idAttributeValue != null
					&& !query.getParameter(idAttributeName).getParameterType().isAssignableFrom(idAttributeValue.getClass());

			if (complexIdParameterValueDiscovered) {

				// fall-back to findById(id) which does the proper mapping for the parameter.
				return findById(id).isPresent();
			}

			query.setParameter(idAttributeName, idAttributeValue);
		}

		return query.getSingleResult() == 1L;
	}

	@Override
	public List<T> findAll() {
		return getQuery(Specification.unrestricted(), Sort.unsorted()).getResultList();
	}

	@Override
	public List<T> findAllById(Iterable<ID> ids) {

		Assert.notNull(ids, IDS_MUST_NOT_BE_NULL);

		if (!ids.iterator().hasNext()) {
			return Collections.emptyList();
		}

		if (entityInformation.hasCompositeId()) {

			List<T> results = new ArrayList<>();

			for (ID id : ids) {
				findById(id).ifPresent(results::add);
			}

			return results;
		}

		Collection<ID> idCollection = toCollection(ids);

		TypedQuery<T> query = getQuery((root, q, criteriaBuilder) -> {

			Path<?> path = root.get(entityInformation.getIdAttribute());
			return path.in(idCollection);

		}, Sort.unsorted());

		return query.getResultList();
	}

	@Override
	public List<T> findAll(Sort sort) {
		return getQuery(Specification.unrestricted(), sort).getResultList();
	}

	@Override
	public Page<T> findAll(Pageable pageable) {
		return findAll(Specification.unrestricted(), pageable);
	}

	@Override
	public Optional<T> findOne(Specification<T> spec) {
		return Optional.ofNullable(getQuery(spec, Sort.unsorted()).setMaxResults(2).getSingleResultOrNull());
	}

	@Override
	public List<T> findAll(Specification<T> spec) {
		return getQuery(spec, Sort.unsorted()).getResultList();
	}

	@Override
	public Page<T> findAll(Specification<T> spec, Pageable pageable) {
		return findAll(spec, spec, pageable);
	}

	@Override
	public Page<T> findAll(@Nullable Specification<T> spec, @Nullable Specification<T> countSpec, Pageable pageable) {

		TypedQuery<T> query = getQuery(spec, pageable);
		return pageable.isUnpaged() ? new PageImpl<>(query.getResultList())
				: readPage(query, getDomainClass(), pageable, countSpec);
	}

	@Override
	public List<T> findAll(Specification<T> spec, Sort sort) {
		return getQuery(spec, sort).getResultList();
	}

	@Override
	public boolean exists(Specification<T> spec) {

		Assert.notNull(spec, "Specification must not be null");

		CriteriaQuery<Integer> cq = this.entityManager.getCriteriaBuilder() //
				.createQuery(Integer.class) //
				.select(this.entityManager.getCriteriaBuilder().literal(1));

		applySpecificationToCriteria(spec, getDomainClass(), cq);

		TypedQuery<Integer> query = applyRepositoryMethodMetadata(this.entityManager.createQuery(cq));
		return query.setMaxResults(1).getResultList().size() == 1;
	}

	@Override
	@Transactional
	public long update(UpdateSpecification<T> spec) {

		Assert.notNull(spec, "Specification must not be null");

		return getUpdate(spec, getDomainClass()).executeUpdate();
	}

	@Override
	@Transactional
	public long delete(DeleteSpecification<T> spec) {

		Assert.notNull(spec, "Specification must not be null");

		return getDelete(spec, getDomainClass()).executeUpdate();
	}

	@Override
	public <S extends T, R> R findBy(Specification<T> spec,
			Function<? super SpecificationFluentQuery<S>, R> queryFunction) {

		Assert.notNull(spec, SPECIFICATION_MUST_NOT_BE_NULL);
		Assert.notNull(queryFunction, QUERY_FUNCTION_MUST_NOT_BE_NULL);

		return doFindBy(spec, getDomainClass(), queryFunction);
	}

	@SuppressWarnings("unchecked")
	private <S extends T, R> R doFindBy(Specification<T> spec, Class<T> domainClass,
			Function<? super SpecificationFluentQuery<S>, R> queryFunction) {

		Assert.notNull(spec, SPECIFICATION_MUST_NOT_BE_NULL);
		Assert.notNull(queryFunction, QUERY_FUNCTION_MUST_NOT_BE_NULL);

		ScrollQueryFactory<TypedQuery<T>> scrollFunction = (q, scrollPosition) -> {

			Specification<T> specToUse = spec;
			Sort sort = q.sort;

			if (scrollPosition instanceof KeysetScrollPosition keyset) {
				KeysetScrollSpecification<T> keysetSpec = new KeysetScrollSpecification<>(keyset, sort, entityInformation);
				sort = keysetSpec.sort();
				specToUse = specToUse.and(keysetSpec);
			}

			TypedQuery<T> query = getQuery(q.returnedType, specToUse, domainClass, sort, q.properties, scrollPosition);

			if (scrollPosition instanceof OffsetScrollPosition offset) {
				if (!offset.isInitial()) {
					query.setFirstResult(Math.toIntExact(offset.getOffset()) + 1);
				}
			}

			return query;
		};

		Function<FluentQuerySupport<?, ?>, TypedQuery<T>> finder = (q) -> getQuery(q.returnedType, spec, domainClass,
				q.sort, q.properties, null);

		SpecificationScrollDelegate<T> scrollDelegate = new SpecificationScrollDelegate<>(scrollFunction,
				entityInformation);
		FetchableFluentQueryBySpecification<?, T> fluentQuery = new FetchableFluentQueryBySpecification<>(spec, domainClass,
				finder, scrollDelegate, this::count, this::exists, this.entityManager, getProjectionFactory());

		R result = queryFunction.apply((SpecificationFluentQuery<S>) fluentQuery);

		if (result instanceof FluentQuery<?>) {
			throw new InvalidDataAccessApiUsageException(
					"findBy(…) queries must result a query result and not the FluentQuery object to ensure that queries are executed within the scope of the findBy(…) method");
		}

		return result;
	}

	@Override
	public <S extends T> Optional<S> findOne(Example<S> example) {

		TypedQuery<S> query = getQuery(new ExampleSpecification<>(example, escapeCharacter), example.getProbeType(),
				Sort.unsorted()).setMaxResults(2);

		return Optional.ofNullable(query.getSingleResultOrNull());
	}

	@Override
	public <S extends T> long count(Example<S> example) {
		return executeCountQuery(
				getCountQuery(new ExampleSpecification<>(example, escapeCharacter), example.getProbeType()));
	}

	@Override
	public <S extends T> boolean exists(Example<S> example) {

		Specification<S> spec = new ExampleSpecification<>(example, this.escapeCharacter);
		CriteriaQuery<Integer> cq = this.entityManager.getCriteriaBuilder() //
				.createQuery(Integer.class) //
				.select(this.entityManager.getCriteriaBuilder().literal(1));

		applySpecificationToCriteria(spec, example.getProbeType(), cq);

		TypedQuery<Integer> query = applyRepositoryMethodMetadata(this.entityManager.createQuery(cq));
		return query.setMaxResults(1).getResultList().size() == 1;
	}

	@Override
	public <S extends T> List<S> findAll(Example<S> example) {
		return getQuery(new ExampleSpecification<>(example, escapeCharacter), example.getProbeType(), Sort.unsorted())
				.getResultList();
	}

	@Override
	public <S extends T> List<S> findAll(Example<S> example, Sort sort) {
		return getQuery(new ExampleSpecification<>(example, escapeCharacter), example.getProbeType(), sort).getResultList();
	}

	@Override
	public <S extends T> Page<S> findAll(Example<S> example, Pageable pageable) {

		ExampleSpecification<S> spec = new ExampleSpecification<>(example, escapeCharacter);
		Class<S> probeType = example.getProbeType();
		TypedQuery<S> query = getQuery(new ExampleSpecification<>(example, escapeCharacter), probeType, pageable);

		return pageable.isUnpaged() ? new PageImpl<>(query.getResultList()) : readPage(query, probeType, pageable, spec);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <S extends T, R> R findBy(Example<S> example, Function<FetchableFluentQuery<S>, R> queryFunction) {

		Assert.notNull(example, EXAMPLE_MUST_NOT_BE_NULL);
		Assert.notNull(queryFunction, QUERY_FUNCTION_MUST_NOT_BE_NULL);

		ExampleSpecification<S> spec = new ExampleSpecification<>(example, escapeCharacter);
		Class<S> probeType = example.getProbeType();

		return doFindBy((Specification<T>) spec, (Class<T>) probeType, queryFunction);
	}

	@Override
	public long count() {

		TypedQuery<Long> query = entityManager.createQuery(countQueryString.get(), Long.class);

		applyQueryHintsForCount(query);

		return query.getSingleResult();
	}

	@Override
	public long count(Specification<T> spec) {
		return executeCountQuery(getCountQuery(spec, getDomainClass()));
	}

	@Override
	@Transactional
	public <S extends T> S save(S entity) {

		Assert.notNull(entity, ENTITY_MUST_NOT_BE_NULL);

		if (entityInformation.isNew(entity)) {
			entityManager.persist(entity);
			return entity;
		} else {
			return entityManager.merge(entity);
		}
	}

	@Override
	@Transactional
	public <S extends T> S saveAndFlush(S entity) {

		S result = save(entity);
		flush();

		return result;
	}

	@Override
	@Transactional
	public <S extends T> List<S> saveAll(Iterable<S> entities) {

		Assert.notNull(entities, ENTITIES_MUST_NOT_BE_NULL);

		List<S> result = new ArrayList<>();

		for (S entity : entities) {
			result.add(save(entity));
		}

		return result;
	}

	@Override
	@Transactional
	public <S extends T> List<S> saveAllAndFlush(Iterable<S> entities) {

		List<S> result = saveAll(entities);
		flush();

		return result;
	}

	@Override
	@Transactional
	public void flush() {
		entityManager.flush();
	}

	/**
	 * Reads the given {@link TypedQuery} into a {@link Page} applying the given {@link Pageable} and
	 * {@link Specification}.
	 *
	 * @param query must not be {@literal null}.
	 * @param spec can be {@literal null}.
	 * @param pageable must not be {@literal null}.
	 * @deprecated use {@link #readPage(TypedQuery, Class, Pageable, Specification)} instead
	 */
	@Deprecated
	protected Page<T> readPage(TypedQuery<T> query, Pageable pageable, Specification<T> spec) {
		return readPage(query, getDomainClass(), pageable, spec);
	}

	/**
	 * Reads the given {@link TypedQuery} into a {@link Page} applying the given {@link Pageable} and
	 * {@link Specification}.
	 *
	 * @param query must not be {@literal null}.
	 * @param domainClass must not be {@literal null}.
	 * @param spec must not be {@literal null}.
	 * @param pageable can be {@literal null}.
	 */
	@Contract("_, _, _, null -> fail")
	protected <S extends T> Page<S> readPage(TypedQuery<S> query, Class<S> domainClass, Pageable pageable,
			@Nullable Specification<S> spec) {

		Assert.notNull(spec, "Specification must not be null");

		if (pageable.isPaged()) {
			query.setFirstResult(PageableUtils.getOffsetAsInteger(pageable));
			query.setMaxResults(pageable.getPageSize());
		}

		return PageableExecutionUtils.getPage(query.getResultList(), pageable,
				() -> executeCountQuery(getCountQuery(spec, domainClass)));
	}

	/**
	 * Creates a new {@link TypedQuery} from the given {@link Specification}.
	 *
	 * @param spec must not be {@literal null}.
	 * @param pageable must not be {@literal null}.
	 */
	protected TypedQuery<T> getQuery(@Nullable Specification<T> spec, Pageable pageable) {
		return getQuery(spec, getDomainClass(), pageable.getSort());
	}

	/**
	 * Creates a new {@link TypedQuery} from the given {@link Specification}.
	 *
	 * @param spec must not be {@literal null}.
	 * @param domainClass must not be {@literal null}.
	 * @param pageable must not be {@literal null}.
	 */
	protected <S extends T> TypedQuery<S> getQuery(Specification<S> spec, Class<S> domainClass, Pageable pageable) {
		return getQuery(spec, domainClass, pageable.getSort());
	}

	/**
	 * Creates a {@link TypedQuery} for the given {@link Specification} and {@link Sort}.
	 *
	 * @param spec must not be {@literal null}.
	 * @param sort must not be {@literal null}.
	 */
	protected TypedQuery<T> getQuery(Specification<T> spec, Sort sort) {
		return getQuery(spec, getDomainClass(), sort);
	}

	/**
	 * Creates a {@link TypedQuery} for the given {@link Specification} and {@link Sort}.
	 *
	 * @param spec must not be {@literal null}.
	 * @param domainClass must not be {@literal null}.
	 * @param sort must not be {@literal null}.
	 */
	protected <S extends T> TypedQuery<S> getQuery(@Nullable Specification<S> spec, Class<S> domainClass, Sort sort) {
		return getQuery(ReturnedType.of(domainClass, domainClass, projectionFactory), spec, domainClass, sort,
				Collections.emptySet(), null);
	}

	/**
	 * Creates a {@link TypedQuery} for the given {@link Specification} and {@link Sort}.
	 *
	 * @param returnedType must not be {@literal null}.
	 * @param spec can be {@literal null}.
	 * @param domainClass must not be {@literal null}.
	 * @param sort must not be {@literal null}.
	 * @param inputProperties must not be {@literal null}.
	 * @param scrollPosition must not be {@literal null}.
	 */
	private <S extends T> TypedQuery<S> getQuery(ReturnedType returnedType, @Nullable Specification<S> spec,
			Class<S> domainClass, Sort sort, Collection<String> inputProperties, @Nullable ScrollPosition scrollPosition) {

		Assert.notNull(spec, "Specification must not be null");

		CriteriaBuilder builder = entityManager.getCriteriaBuilder();
		CriteriaQuery<S> query;

		boolean interfaceProjection = returnedType.getReturnedType().isInterface();

		if (returnedType.needsCustomConstruction() && (inputProperties.isEmpty() || !interfaceProjection)) {
			inputProperties = returnedType.getInputProperties();
		}

		if (returnedType.needsCustomConstruction()) {
			query = (CriteriaQuery) (interfaceProjection ? builder.createTupleQuery()
					: builder.createQuery(returnedType.getReturnedType()));
		} else {
			query = builder.createQuery(domainClass);
		}

		Root<S> root = applySpecificationToCriteria(spec, domainClass, query);

		if (returnedType.needsCustomConstruction()) {

			Collection<String> requiredSelection;

			if (scrollPosition instanceof KeysetScrollPosition && interfaceProjection) {
				requiredSelection = KeysetScrollDelegate.getProjectionInputProperties(entityInformation, inputProperties, sort);
			} else {
				requiredSelection = inputProperties;
			}

			List<Selection<?>> selections = new ArrayList<>();
			Set<String> topLevelProperties = new HashSet<>();
			for (String property : requiredSelection) {

				int separator = property.indexOf('.');
				String topLevelProperty = separator == -1 ? property : property.substring(0, separator);

				if (!topLevelProperties.add(topLevelProperty)) {
					continue;
				}

				PropertyPath path = PropertyPath.from(topLevelProperty, returnedType.getDomainType());
				selections.add(QueryUtils.toExpressionRecursively(root, path, true).alias(topLevelProperty));
			}

			Class<?> typeToRead = returnedType.getReturnedType();

			query = typeToRead.isInterface() //
					? query.multiselect(selections) //
					: query.select((Selection) builder.construct(typeToRead, //
							selections.toArray(new Selection[0])));
		} else {
			query.select(root);
		}

		if (sort.isSorted()) {
			query.orderBy(toOrders(sort, root, builder));
		}

		return applyRepositoryMethodMetadata(entityManager.createQuery(query));
	}

	/**
	 * Creates a {@link Query} for the given {@link UpdateSpecification}.
	 *
	 * @param spec must not be {@literal null}.
	 * @param domainClass must not be {@literal null}.
	 */
	protected <S> Query getUpdate(UpdateSpecification<S> spec, Class<S> domainClass) {

		Assert.notNull(spec, "Specification must not be null");

		CriteriaBuilder builder = entityManager.getCriteriaBuilder();
		CriteriaUpdate<S> query = builder.createCriteriaUpdate(domainClass);

		applySpecificationToCriteria(spec, domainClass, query);

		return applyRepositoryMethodMetadata(entityManager.createQuery(query));
	}

	/**
	 * Creates a {@link Query} for the given {@link DeleteSpecification}.
	 *
	 * @param spec must not be {@literal null}.
	 * @param domainClass must not be {@literal null}.
	 */
	protected <S> Query getDelete(DeleteSpecification<S> spec, Class<S> domainClass) {

		Assert.notNull(spec, "Specification must not be null");

		CriteriaBuilder builder = entityManager.getCriteriaBuilder();
		CriteriaDelete<S> query = builder.createCriteriaDelete(domainClass);

		applySpecificationToCriteria(spec, domainClass, query);

		return applyRepositoryMethodMetadata(entityManager.createQuery(query));
	}

	/**
	 * Creates a new count query for the given {@link Specification}.
	 *
	 * @param spec must not be {@literal null}.
	 * @deprecated override {@link #getCountQuery(Specification, Class)} instead
	 */
	@Deprecated
	protected TypedQuery<Long> getCountQuery(Specification<T> spec) {
		return getCountQuery(spec, getDomainClass());
	}

	/**
	 * Creates a new count query for the given {@link Specification}.
	 *
	 * @param spec must not be {@literal null}.
	 * @param domainClass must not be {@literal null}.
	 */
	protected <S extends T> TypedQuery<Long> getCountQuery(Specification<S> spec, Class<S> domainClass) {

		Assert.notNull(spec, "Specification must not be null");

		CriteriaBuilder builder = entityManager.getCriteriaBuilder();
		CriteriaQuery<Long> query = builder.createQuery(Long.class);

		Root<S> root = applySpecificationToCriteria(spec, domainClass, query);

		if (query.isDistinct()) {
			query.select(builder.countDistinct(root));
		} else {
			query.select(builder.count(root));
		}

		// Remove all Orders the Specifications might have applied
		query.orderBy(Collections.emptyList());

		return applyRepositoryMethodMetadataForCount(entityManager.createQuery(query));
	}

	/**
	 * Returns {@link QueryHints} with the query hints based on the current {@link CrudMethodMetadata} and potential
	 * {@link EntityGraph} information.
	 */
	protected QueryHints getQueryHints() {
		return metadata == null ? NoHints.INSTANCE : DefaultQueryHints.of(entityInformation, metadata);
	}

	/**
	 * Returns {@link QueryHints} with the query hints on the current {@link CrudMethodMetadata} for count queries.
	 */
	protected QueryHints getQueryHintsForCount() {
		return metadata == null ? NoHints.INSTANCE : DefaultQueryHints.of(entityInformation, metadata).forCounts();
	}

	private <S, U extends T> Root<U> applySpecificationToCriteria(Specification<U> spec, Class<U> domainClass,
			CriteriaQuery<S> query) {

		Root<U> root = query.from(domainClass);

		CriteriaBuilder builder = entityManager.getCriteriaBuilder();
		Predicate predicate = spec.toPredicate(root, query, builder);

		if (predicate != null) {
			query.where(predicate);
		}

		return root;
	}

	private <S> void applySpecificationToCriteria(UpdateSpecification<S> spec, Class<S> domainClass,
			CriteriaUpdate<S> query) {

		Root<S> root = query.from(domainClass);

		CriteriaBuilder builder = entityManager.getCriteriaBuilder();
		Predicate predicate = spec.toPredicate(root, query, builder);

		if (predicate != null) {
			query.where(predicate);
		}
	}

	private <S> void applySpecificationToCriteria(DeleteSpecification<S> spec, Class<S> domainClass,
			CriteriaDelete<S> query) {

		Root<S> root = query.from(domainClass);

		CriteriaBuilder builder = entityManager.getCriteriaBuilder();
		Predicate predicate = spec.toPredicate(root, query, builder);

		if (predicate != null) {
			query.where(predicate);
		}
	}

	private <S> TypedQuery<S> applyRepositoryMethodMetadata(TypedQuery<S> query) {

		if (metadata == null) {
			return query;
		}

		LockModeType type = metadata.getLockModeType();
		TypedQuery<S> toReturn = type == null ? query : query.setLockMode(type);

		applyQueryHints(toReturn);

		return toReturn;
	}

	private Query applyRepositoryMethodMetadata(Query query) {

		if (metadata == null) {
			return query;
		}

		LockModeType type = metadata.getLockModeType();
		Query toReturn = type == null ? query : query.setLockMode(type);

		applyQueryHints(toReturn);

		return toReturn;
	}

	private void applyQueryHints(Query query) {

		if (metadata == null) {
			return;
		}

		getQueryHints().withFetchGraphs(entityManager).forEach(query::setHint);
		applyComment(metadata, query::setHint);
	}

	private <S> TypedQuery<S> applyRepositoryMethodMetadataForCount(TypedQuery<S> query) {

		if (metadata == null) {
			return query;
		}

		applyQueryHintsForCount(query);

		return query;
	}

	private void applyQueryHintsForCount(Query query) {

		if (metadata == null) {
			return;
		}

		getQueryHintsForCount().forEach(query::setHint);
		applyComment(metadata, query::setHint);
	}

	private Map<String, Object> getHints() {

		Map<String, Object> hints = new HashMap<>();

		getQueryHints().withFetchGraphs(entityManager).forEach(hints::put);

		if (metadata != null) {
			applyComment(metadata, hints::put);
		}

		return hints;
	}

	private void applyComment(CrudMethodMetadata metadata, BiConsumer<String, Object> consumer) {

		if (metadata.getComment() != null && provider.getCommentHintKey() != null) {
			consumer.accept(provider.getCommentHintKey(), provider.getCommentHintValue(metadata.getComment()));
		}
	}

	private ProjectionFactory getProjectionFactory() {

		if (projectionFactory == null) {
			projectionFactory = new SpelAwareProxyProjectionFactory();
		}

		return projectionFactory;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static <T> Collection<T> toCollection(Iterable<T> ids) {
		return ids instanceof Collection c ? c : Streamable.of(ids).toList();
	}

	/**
	 * Executes a count query and transparently sums up all values returned.
	 *
	 * @param query must not be {@literal null}.
	 */
	private static long executeCountQuery(TypedQuery<Long> query) {

		Assert.notNull(query, "TypedQuery must not be null");

		List<Long> totals = query.getResultList();
		long total = 0L;

		for (Long element : totals) {
			total += element == null ? 0 : element;
		}

		return total;
	}

	/**
	 * {@link Specification} that gives access to the {@link Predicate} instance representing the values contained in the
	 * {@link Example}.
	 *
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 1.10
	 */
	private record ExampleSpecification<T>(Example<T> example,
			EscapeCharacter escapeCharacter) implements Specification<T> {

		/**
		 * Creates new {@link ExampleSpecification}.
		 *
		 * @param example the example to base the specification of. Must not be {@literal null}.
		 * @param escapeCharacter the escape character to use for like expressions. Must not be {@literal null}.
		 */
		private ExampleSpecification {

			Assert.notNull(example, EXAMPLE_MUST_NOT_BE_NULL);
			Assert.notNull(escapeCharacter, "EscapeCharacter must not be null");

		}

		@Override
		public @Nullable Predicate toPredicate(Root<T> root, @Nullable CriteriaQuery<?> query, CriteriaBuilder cb) {
			return QueryByExamplePredicateBuilder.getPredicate(root, cb, example, escapeCharacter);
		}
	}
}
