package org.ff4j.cassandra.store;

/*
 * #%L
 * ff4j-store-cassandra
 * %%
 * Copyright (C) 2013 - 2016 FF4J
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Query;
import org.ff4j.cassandra.CassandraConstants;
import static org.ff4j.cassandra.CassandraConstants.COLUMN_FAMILY_FEATURES;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.ff4j.cassandra.CassandraConnection;
import org.ff4j.cassandra.CassandraQueryBuilder;
import org.ff4j.cassandra.FeatureDto;
import org.ff4j.core.Feature;
import org.ff4j.core.FeatureStore;
import org.ff4j.exception.FeatureNotFoundException;
import org.ff4j.store.AbstractFeatureStore;
import org.ff4j.utils.Util;

/**
 * Implementation of {@link FeatureStore} to work with Cassandra Storage.
 * 
 * Minimize the Number of Writes : 
 * Writes in Cassandra aren’t free, but they’re awfully cheap. Cassandra is optimized for high write throughput, 
 * and almost all writes are equally efficient [1]. If you can perform extra writes to improve the efficiency of
 * your read queries, it’s almost always a good tradeoff. Reads tend to be more expensive and are much more 
 * difficult to tune.
 * 
 * Minimize Data Duplication
 * Denormalization and duplication of data is a fact of life with Cassandra. Don’t be afraid of it. Disk space 
 * is generally the cheapest resource (compared to CPU, memory, disk IOPs, or network), and Cassandra is 
 * architected around that fact. In order to get the most efficient reads, you often need to duplicate data.
 * 
 * Rule 1: Spread Data Evenly Around the Cluster
 * Rule 2: Minimize the Number of Partitions Read
 * 
 * Ces familles de colonnes contiennent des colonnes ainsi qu'un ensemble de colonnes connexes qui sont identifiées par une clé de ligne
 * 
 *
 * @author Cedrick Lunven (@clunven)
 */
public class FeatureStoreCassandra extends AbstractFeatureStore {
    
    /** Connection to store Cassandra. */
    private CassandraQueryBuilder builder;
            
    /** Connection to store Cassandra. */
    private CassandraConnection conn;

	private Session session;
	private MappingManager manager;
	private Mapper<FeatureDto> mapper;
	private BatchAccessor batchAccessor;
	private GroupAccessor groupAccessor;

    /**
     * Default constructor.
     */
    public FeatureStoreCassandra() {
    	// XXX session. manager, connection not set?
    }
    
    /**
     * Initialization through {@link CassandraConnection}.
     *
     * @param conn
     *      current client to cassandra db
     */
    public FeatureStoreCassandra(CassandraConnection conn) {
        this.conn = conn;
			this.session = conn.getSession();
			this.manager = new MappingManager(session);
			this.mapper = manager.mapper(FeatureDto.class);
			this.batchAccessor = manager.createAccessor(BatchAccessor.class);
			this.groupAccessor = manager.createAccessor(GroupAccessor.class);
		}

    /** {@inheritDoc} */
    @Override
    public void createSchema() {
       // Roles & custom properties will be in the same column family  
       if (!conn.isColumnFamilyExist(COLUMN_FAMILY_FEATURES)) {
           
           // Create table
           conn.getSession().execute(getBuilder().cqlCreateColumnFamilyFeature());
           
           // Add a secondary index to query
           conn.getSession().execute(getBuilder().cqlCreateIndexGroupName());
       }
    }
    
    /** {@inheritDoc} */
    @Override
    public boolean exist(String uid) {
			Util.assertHasLength(uid);
			return mapper.get(uid) != null;
    }
    
    /** {@inheritDoc} */
    @Override
    public void enable(String uid) {
			FeatureDto f = existingFeature(uid);
			f.setEnable(1);
			mapper.save(f);
    }

    /** {@inheritDoc} */
    @Override
    public void disable(String uid) {
        FeatureDto f = existingFeature(uid);
        f.setEnable(0);
        mapper.save(f);
    }    

    /** {@inheritDoc} */
    @Override
    public void create(Feature f) {
        assertFeatureNotNull(f);
        assertFeatureNotExist(f.getUid());
        FeatureDto dto = FeatureDto.fromFeature(f);
        mapper.save(dto);
    }
    
    /** {@inheritDoc} */
    @Override
    public void delete(String uid) {
			mapper.delete(existingFeature(uid));
    }

	/** {@inheritDoc} */
    @Override
    public Feature read(String uid) {
			return existingFeature(uid).asFeature();
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, Feature> readAll() {
			Map < String, Feature> features = new HashMap<String, Feature>();
    	for (FeatureDto dto : batchAccessor.getAll()) {
    		Feature f = dto.asFeature();
    		features.put(f.getUid(), f);
			}
			return features;
    }
    
    /** {@inheritDoc} */
    @Override
    public void update(Feature f) {
        assertFeatureNotNull(f);
        assertFeatureExist(f.getUid());
        // easiest way to perform delta update (lot of attributes)
			// XXX well... i disagree, why not just update?
        delete(f.getUid());
        create(f);
    }

    /** {@inheritDoc} */
    @Override
    public void grantRoleOnFeature(String uid, String roleName) {
        Util.assertHasLength(roleName);
        FeatureDto f = existingFeature(uid);
        f.getRoles().add(roleName);
        mapper.save(f);
    }

    /** {@inheritDoc} */
    @Override
    public void removeRoleFromFeature(String uid, String roleName) {
			Util.assertHasLength(roleName);
			FeatureDto f = existingFeature(uid);
			f.getRoles().remove(roleName);
			mapper.save(f);
    }
    
    /** {@inheritDoc} */
    @Override
    public void enableGroup(String groupName) {
        assertGroupExist(groupName);
        /* Even with secondary index the 'update SET enable =1 WHERE GROUPNAME=?' does not work
         * We will update each feature one by one
         */
			for (FeatureDto f : groupAccessor.findByGroup(groupName)) {
				f.setEnable(1);
				mapper.save(f);
			}
    }

    /** {@inheritDoc} */
    @Override
    public void disableGroup(String groupName) {
        assertGroupExist(groupName);
			for (FeatureDto f : groupAccessor.findByGroup(groupName)) {
				f.setEnable(0);
				mapper.save(f);
			}
    }

    /** {@inheritDoc} */
    @Override
    public boolean existGroup(String groupName) {
        Util.assertHasLength(groupName);
        Result<FeatureDto> feature = groupAccessor.findByGroup(groupName); // XXX limit 1?
			return feature.one() != null;
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, Feature> readGroup(String groupName) {
        assertGroupExist(groupName);
        Map<String, Feature> result = new HashMap<String, Feature>();
			for (FeatureDto dto : groupAccessor.findByGroup(groupName)) {
				Feature f = dto.asFeature();
				result.put(f.getUid(), f);
			}
        return result;
    }

    /** {@inheritDoc} */
    @Override
    public void addToGroup(String uid, String groupName) {
    	FeatureDto f = existingFeature(uid);
        Util.assertHasLength(groupName);
        f.setGroupName(groupName);
        mapper.save(f);
    }

    /** {@inheritDoc} */
    @Override
    public void removeFromGroup(String uid, String groupName) {
			FeatureDto f = existingFeature(uid);
        assertGroupExist(groupName);
			f.setGroupName(null);
			mapper.save(f);
    }

    /** {@inheritDoc} */
    @Override
    public Set<String> readAllGroups() {
			Set<String> groups = new HashSet<String>();
			for (FeatureDto f : batchAccessor.getAll()) {
				groups.add(f.getGroupName());
			}
			groups.remove(null);
			groups.remove("");
			return groups;
    }

    /** {@inheritDoc} */
    @Override
    public void clear() {
        conn.getSession().execute(getBuilder().cqlTruncateFeatures());
    }

    /**
     * Getter accessor for attribute 'conn'.
     *
     * @return
     *       current value of 'conn'
     */
    public CassandraConnection getConn() {
        return conn;
    }

    /**
     * Setter accessor for attribute 'conn'.
     * @param conn
     * 		new value for 'conn '
     */
    public void setConn(CassandraConnection conn) {
        this.conn = conn;
    }

    /**
     * Getter accessor for attribute 'builder'.
     *
     * @return
     *       current value of 'builder'
     */
    public CassandraQueryBuilder getBuilder() {
        if (builder == null) {
            builder = new CassandraQueryBuilder(conn);
        }
        return builder;
    }

	private FeatureDto existingFeature(String uid) {
		Util.assertHasLength(uid);
		FeatureDto f = mapper.get(uid);
		if (f == null) {
			throw new FeatureNotFoundException(uid);
		}
		return f;
	}

	@Accessor
	private interface BatchAccessor {
		@Query("SELECT * FROM " + CassandraConstants.KEYSPACE + "." + CassandraConstants.COLUMN_FAMILY_FEATURES)
		Result<FeatureDto> getAll();
	}

	@Accessor
	interface GroupAccessor {
		@Query("SELECT * FROM " + CassandraConstants.KEYSPACE + "." + CassandraConstants.COLUMN_FAMILY_FEATURES
				+ " WHERE groupname = ?")
		Result<FeatureDto> findByGroup(String group);
	}

}
