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

package org.springframework.cloud.consul.config;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetValue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.bootstrap.config.PropertySourceLocator;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.CompositePropertySource;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;
import org.springframework.retry.annotation.Retryable;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import java.util.*;

import static org.springframework.cloud.consul.config.ConsulConfigProperties.Format.FILES;

/**
 * @author Spencer Gibb
 */
@Order(0)
public class ConsulPropertySourceLocator implements PropertySourceLocator {

	private static final Log log = LogFactory.getLog(ConsulPropertySourceLocator.class);

	private final ConsulClient consul;

	private final ConsulConfigProperties properties;

	private final List<String> contexts = new ArrayList<>();

	private final LinkedHashMap<String, Long> contextIndex = new LinkedHashMap<>();

	public ConsulPropertySourceLocator(ConsulClient consul,
	                                   ConsulConfigProperties properties) {
		this.consul = consul;
		this.properties = properties;
	}

	@Deprecated
	public List<String> getContexts() {
		return this.contexts;
	}

	public LinkedHashMap<String, Long> getContextIndexes() {
		return this.contextIndex;
	}

	@Override
	@Retryable(interceptor = "consulRetryInterceptor")
	public Collection<PropertySource<?>> locateCollection(Environment environment) {
		return PropertySourceLocator.locateCollection(this, environment);
	}

	/**
	 * 拉取配置文件解析并添加到容器中
	 *
	 * @param environment
	 * @return
	 */
	@Override
	@Retryable(interceptor = "consulRetryInterceptor")
	public PropertySource<?> locate(Environment environment) {
		if (environment instanceof ConfigurableEnvironment) {
			ConfigurableEnvironment env = (ConfigurableEnvironment) environment;

			String appName = this.properties.getName();

			if (appName == null) {
				appName = env.getProperty("spring.application.name");
			}

			List<String> profiles = Arrays.asList(env.getActiveProfiles());

			String prefix = this.properties.getPrefix();

			List<String> suffixes = new ArrayList<>();
			// 不是文件类型的时候，后缀为 /，否则就是配置文件的后缀
			if (this.properties.getFormat() != FILES) {
				suffixes.add("/");
			} else {
				suffixes.add(".yml");
				suffixes.add(".yaml");
				suffixes.add(".properties");
			}

			// 路径
			String defaultContext = getContext(prefix, this.properties.getDefaultContext());

			for (String suffix : suffixes) {
				this.contexts.add(defaultContext + suffix);
			}
			// 追加环境及文件类型
			for (String suffix : suffixes) {
				addProfiles(this.contexts, defaultContext, profiles, suffix);
			}

			String baseContext = getContext(prefix, appName);

			// 应用名称前缀
			for (String suffix : suffixes) {
				this.contexts.add(baseContext + suffix);
			}
			for (String suffix : suffixes) {
				addProfiles(this.contexts, baseContext, profiles, suffix);
			}

			Collections.reverse(this.contexts);

			CompositePropertySource composite = new CompositePropertySource("consul");

			for (String propertySourceContext : this.contexts) {
				try {
					ConsulPropertySource propertySource = null;
					if (this.properties.getFormat() == FILES) {
						// 获取值
						Response<GetValue> response = this.consul.getKVValue(propertySourceContext, this.properties.getAclToken());
						// 添加当前索引
						addIndex(propertySourceContext, response.getConsulIndex());
						// 如果值不为空，则更新值并初始化
						if (response.getValue() != null) {
							ConsulFilesPropertySource filesPropertySource = new ConsulFilesPropertySource(propertySourceContext, this.consul, this.properties);
							// 解析配置内容
							filesPropertySource.init(response.getValue());
							propertySource = filesPropertySource;
						}
					} else {
						propertySource = create(propertySourceContext, this.contextIndex);
					}
					if (propertySource != null) {
						composite.addPropertySource(propertySource);
					}
				} catch (Exception e) {
					if (this.properties.isFailFast()) {
						log.error("Fail fast is set and there was an error reading configuration from consul.");
						ReflectionUtils.rethrowRuntimeException(e);
					} else {
						log.warn("Unable to load consul config from " + propertySourceContext, e);
					}
				}
			}

			return composite;
		}
		return null;
	}

	private String getContext(String prefix, String context) {
		if (StringUtils.isEmpty(prefix)) {
			return context;
		} else {
			return prefix + "/" + context;
		}
	}

	private void addIndex(String propertySourceContext, Long consulIndex) {
		this.contextIndex.put(propertySourceContext, consulIndex);
	}

	private ConsulPropertySource create(String context, Map<String, Long> contextIndex) {
		ConsulPropertySource propertySource = new ConsulPropertySource(context, this.consul, this.properties);
		propertySource.init();
		addIndex(context, propertySource.getInitialIndex());
		return propertySource;
	}

	/**
	 * 将拉取的配置添加到容器中
	 *
	 * @param contexts
	 * @param baseContext
	 * @param profiles
	 * @param suffix
	 */
	private void addProfiles(List<String> contexts, String baseContext,
	                         List<String> profiles, String suffix) {
		for (String profile : profiles) {
			contexts.add(baseContext + this.properties.getProfileSeparator() + profile + suffix);
		}
	}

}
