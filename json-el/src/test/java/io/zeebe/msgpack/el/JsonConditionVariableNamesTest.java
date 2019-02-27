/*
 * Copyright © 2019  camunda services GmbH (info@camunda.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package io.zeebe.msgpack.el;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import scala.collection.JavaConverters;
import scala.collection.mutable.HashSet;

@RunWith(Parameterized.class)
public class JsonConditionVariableNamesTest {

  @Parameters(name = "{index}: expression = {0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {"$.foo == $.bar || $.foo > 2 || $.bar <= 2", new String[] {"$"}},
          {"foo == bar || foo > 2 || bar <= 2", new String[] {"foo", "bar"}},
          {"$.foo == true", new String[] {"$"}},
          {"foo == true", new String[] {"foo"}},
          {"$.foo == 21", new String[] {"$"}},
          {"$.foo == 2.5", new String[] {"$"}},
          {"$.foo == $.bar", new String[] {"$"}},
          {"$.foo.bar == true", new String[] {"$"}},
          {"$.foo[1] == true", new String[] {"$"}},
          {"'foo' == 'bar'", new String[0]},
          {"$.foo != 'bar'", new String[] {"$"}},
          {"foo >= bar", new String[] {"foo", "bar"}},
          {"foo.bar >= bar.foo", new String[] {"foo", "bar"}},
          {"2 < 4", new String[0]},
          {"foo.bar > 2 || bar.foo < 4 || foobar == 21", new String[] {"foo", "bar", "foobar"}},
          {"foo > 2 && foo < 4 || bar == 6", new String[] {"foo", "bar"}},
          {"(foo == 2)", new String[] {"foo"}},
          {"$.foo > 2 && ($.foo < 4 || $.bar == 6)", new String[] {"$"}}
        });
  }

  @Parameter public String expression;

  @Parameter(1)
  public String[] expectedVariableNames;

  @Test
  public void shouldReturnOnlyUniqueVariableNames() {
    final CompiledJsonCondition condition = JsonConditionFactory.createCondition(expression);
    assertThat(condition.isValid()).isTrue();

    final HashSet<String> actualVariables = condition.getCondition().variableNames();
    assertThat(JavaConverters.asJavaCollection(actualVariables))
        .containsExactlyInAnyOrder(expectedVariableNames);
  }
}
