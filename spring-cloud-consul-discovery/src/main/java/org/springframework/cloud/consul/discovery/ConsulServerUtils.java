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

import com.ecwid.consul.v1.health.model.HealthService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.StringUtils;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Spencer Gibb
 * @author Semenkov Alexey
 */
public final class ConsulServerUtils {

	private static final Log log = LogFactory.getLog(ConsulServerUtils.class);

	private ConsulServerUtils() {
		throw new IllegalStateException("Can't instantiate a utility class");
	}

	/**
	 * 根据 service 的数据生成相应的 ip 地址
	 * @param healthService
	 * @return
	 */
	public static String findHost(HealthService healthService) {
		HealthService.Service service = healthService.getService();
		HealthService.Node node = healthService.getNode();

		// service 地址优先，如果没有则使用 node 地址，如果都没有，则使用 node 名称
		if (StringUtils.hasText(service.getAddress())) {
			return fixIPv6Address(service.getAddress());
		} else if (StringUtils.hasText(node.getAddress())) {
			return fixIPv6Address(node.getAddress());
		}
		return node.getNode();
	}

	public static String fixIPv6Address(String address) {
		try {
			InetAddress inetAdr = InetAddress.getByName(address);
			if (inetAdr instanceof Inet6Address) {
				return "[" + inetAdr.getHostName() + "]";
			}
			return address;
		} catch (UnknownHostException e) {
			log.debug("Not InetAddress: " + address + " , resolved as is.");
			return address;
		}
	}

	@Deprecated
	public static Map<String, String> getMetadata(HealthService healthService) {
		return getMetadata(healthService, true);
	}

	@Deprecated
	public static Map<String, String> getMetadata(HealthService healthService,
	                                              boolean tagsAsMetadata) {
		if (tagsAsMetadata) {
			return getMetadata(healthService.getService().getTags());
		}
		return healthService.getService().getMeta();
	}

	@Deprecated
	public static Map<String, String> getMetadata(List<String> tags) {
		LinkedHashMap<String, String> metadata = new LinkedHashMap<>();
		if (tags != null) {
			for (String tag : tags) {
				String[] parts = StringUtils.delimitedListToStringArray(tag, "=");
				switch (parts.length) {
					case 0:
						break;
					case 1:
						metadata.put(parts[0], parts[0]);
						break;
					case 2:
						metadata.put(parts[0], parts[1]);
						break;
					default:
						String[] end = Arrays.copyOfRange(parts, 1, parts.length);
						metadata.put(parts[0], StringUtils.arrayToDelimitedString(end, "="));
						break;
				}

			}
		}

		return metadata;
	}

}
