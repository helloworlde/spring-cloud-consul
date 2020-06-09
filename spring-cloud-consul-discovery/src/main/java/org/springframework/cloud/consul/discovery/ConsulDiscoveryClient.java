/*
 * Copyright 2013-2019 the original author or authors.
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

package org.springframework.cloud.consul.discovery;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.catalog.CatalogServicesRequest;
import com.ecwid.consul.v1.health.HealthServicesRequest;
import com.ecwid.consul.v1.health.model.HealthService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.springframework.cloud.consul.discovery.ConsulServerUtils.findHost;
import static org.springframework.cloud.consul.discovery.ConsulServerUtils.getMetadata;

/**
 * @author Spencer Gibb
 * @author Joe Athman
 * @author Tim Ysewyn
 */
public class ConsulDiscoveryClient implements DiscoveryClient {

	private static final Log log = LogFactory.getLog(ConsulDiscoveryClient.class);

	private final ConsulClient client;

	private final ConsulDiscoveryProperties properties;

	public ConsulDiscoveryClient(ConsulClient client,
	                             ConsulDiscoveryProperties properties) {
		this.client = client;
		this.properties = properties;
	}

	@Override
	public String description() {
		return "Spring Cloud Consul Discovery Client";
	}

	/**
	 * 根据服务id获取服务
	 *
	 * @param serviceId
	 * @return
	 */
	@Override
	public List<ServiceInstance> getInstances(final String serviceId) {
		return getInstances(serviceId,
			new QueryParams(this.properties.getConsistencyMode()));
	}

	/**
	 * 根据实例的 id，查询单个实例
	 *
	 * @param serviceId
	 * @param queryParams
	 * @return
	 */
	public List<ServiceInstance> getInstances(final String serviceId,
	                                          final QueryParams queryParams) {
		List<ServiceInstance> instances = new ArrayList<>();

		addInstancesToList(instances, serviceId, queryParams);

		return instances;
	}

	/**
	 * 根据 Service 信息，生成 service 的实例
	 *
	 * @param instances
	 * @param serviceId
	 * @param queryParams
	 */
	private void addInstancesToList(List<ServiceInstance> instances, String serviceId,
	                                QueryParams queryParams) {

		// 请求参数
		HealthServicesRequest request = HealthServicesRequest.newBuilder()
		                                                     .setTag(this.properties.getDefaultQueryTag())
		                                                     .setPassing(this.properties.isQueryPassing())
		                                                     .setQueryParams(queryParams)
		                                                     .setToken(this.properties.getAclToken()).build();
		Response<List<HealthService>> services = this.client.getHealthServices(serviceId,
			request);

		for (HealthService service : services.getValue()) {
			String host = findHost(service);

			Map<String, String> metadata = getMetadata(service, this.properties.isTagsAsMetadata());
			boolean secure = false;
			if (metadata.containsKey("secure")) {
				secure = Boolean.parseBoolean(metadata.get("secure"));
			}
			instances.add(
				new DefaultServiceInstance(
					service.getService().getId(),
					serviceId,
					host,
					service.getService().getPort(),
					secure,
					metadata)
			);
		}
	}

	/**
	 * 获取所有的实例列表
	 *
	 * @return
	 */
	public List<ServiceInstance> getAllInstances() {
		List<ServiceInstance> instances = new ArrayList<>();

		Response<Map<String, List<String>>> services = this.client
			.getCatalogServices(CatalogServicesRequest.newBuilder()
			                                          .setQueryParams(QueryParams.DEFAULT)
			                                          .build());
		for (String serviceId : services.getValue().keySet()) {
			addInstancesToList(instances, serviceId, QueryParams.DEFAULT);
		}
		return instances;
	}

	/**
	 * 获取所有的服务名称列表
	 *
	 * @return
	 */
	@Override
	public List<String> getServices() {
		String aclToken = this.properties.getAclToken();

		CatalogServicesRequest request = CatalogServicesRequest.newBuilder()
		                                                       .setQueryParams(QueryParams.DEFAULT)
		                                                       .setToken(this.properties.getAclToken())
		                                                       .build();
		return new ArrayList<>(this.client.getCatalogServices(request).getValue().keySet());
	}

	@Override
	public int getOrder() {
		return this.properties.getOrder();
	}

	/**
	 * Depreacted local resolver.
	 */
	@Deprecated
	public interface LocalResolver {

		String getInstanceId();

		Integer getPort();

	}

}
