package org.ff4j.cassandra;

/*
 * #%L
 * ff4j-store-cassandra
 * %%
 * Copyright (C) 2013 - 2018 FF4J
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

import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.ff4j.core.Feature;
import org.ff4j.property.Property;
import org.ff4j.utils.JsonUtils;
import org.ff4j.utils.Util;
import org.ff4j.utils.json.FeatureJsonParser;
import org.ff4j.utils.json.PropertyJsonParser;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Table(keyspace = CassandraConstants.KEYSPACE, name = CassandraConstants.COLUMN_FAMILY_FEATURES)
public class FeatureDto {
	@PartitionKey(0)
	private String uid;
	private int enable;
	private String description;
	private String strategy;
	private String groupName;
	private Set<String> roles = new HashSet<String>();
	private Map<String, String> properties = new HashMap<String, String>();

	public static FeatureDto fromFeature(Feature feature) {
		FeatureDto dto = new FeatureDto();
		dto.setUid(feature.getUid());
		dto.setDescription(feature.getDescription());
		dto.setGroupName(feature.getGroup());
		dto.setEnable(feature.isEnable() ? 1 : 0);
		String strategy = JsonUtils.flippingStrategyAsJson(feature.getFlippingStrategy());
		dto.setStrategy(strategy);
		dto.setRoles(feature.getPermissions());
		Map<String, String> mapOfProperties = new HashMap<String, String>();
		if (feature.getCustomProperties() != null && !feature.getCustomProperties().isEmpty()) {
			for (Map.Entry<String, Property<?>> customP : feature.getCustomProperties().entrySet()) {
				if (customP.getValue() != null) {
					mapOfProperties.put(customP.getKey(), customP.getValue().toJson());
				}
			}
		}
		dto.setProperties(mapOfProperties);
		return dto;
	}

	public Feature asFeature() {
		Feature f = new Feature(uid);
		f.setGroup(groupName);
		f.setDescription(description);
		f.setEnable(enable != 0);
		f.setPermissions(roles);
		// Custom Properties
		if (properties != null) {
			Map < String, Property<?>> customProperties = new HashMap<String, Property<?>>();
			for (Map.Entry<String, String> propString : properties.entrySet()) {
				customProperties.put(propString.getKey(), PropertyJsonParser.parseProperty(propString.getValue()));
			}
			f.setCustomProperties(customProperties);
		}
		// Flipping Strategy
		if (Util.hasLength(strategy)) {
			f.setFlippingStrategy(FeatureJsonParser.parseFlipStrategyAsJson(f.getUid(), strategy));
		}
		return f;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public int getEnable() {
		return enable;
	}

	public void setEnable(int enable) {
		this.enable = enable;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getStrategy() {
		return strategy;
	}

	public void setStrategy(String strategy) {
		this.strategy = strategy;
	}

	public String getGroupName() {
		return groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	public Set<String> getRoles() {
		return roles;
	}

	public void setRoles(Set<String> roles) {
		this.roles = roles;
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	public void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}
}
