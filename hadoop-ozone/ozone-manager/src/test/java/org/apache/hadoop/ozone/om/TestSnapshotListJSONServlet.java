/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.snapshot.ListSnapshotResponse;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

/**
 * Test for SnapshotListJSONServlet.
 */
public class TestSnapshotListJSONServlet {

  @Test
  public void testSerialization() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.addMixIn(SnapshotInfo.class,
        SnapshotListJSONServlet.SnapshotInfoMixin.class);

    String snapName = "snap1";
    SnapshotInfo snapshotInfo = SnapshotInfo.newInstance("vol1", "bucket1",
        snapName, UUID.randomUUID(), Time.now());

    String json = assertDoesNotThrow(() ->
        objectMapper.writeValueAsString(Collections.singletonList(snapshotInfo)));

    // Verify that the problematic fields are NOT in the JSON
    assertFalse(json.contains("protobuf"), "JSON should not contain protobuf field");
    assertFalse(json.contains("createTransactionInfo"), "JSON should not contain createTransactionInfo field");
    assertFalse(json.contains("lastTransactionInfo"), "JSON should not contain lastTransactionInfo field");

    // Verify that the expected fields ARE in the JSON
    assertTrue(json.contains("\"name\":\"" + snapName + "\""), "JSON should contain snapshot name");
  }

  @Test
  public void testListSnapshotRunsAsHttpUser() throws Exception {
    OzoneManager om = mockOzoneManager(true);
    List<String> users = recordListSnapshotUsers(om);
    HttpServletResponse response = doGet(om, "alice");

    verify(response, never()).setStatus(anyInt());
    assertThat(users).containsExactly("alice");
  }

  @Test
  public void testListSnapshotPermissionDenied() throws Exception {
    OzoneManager om = mockOzoneManager(true);
    when(om.listSnapshot(any(), any(), any(), any(), anyInt()))
        .thenThrow(new OMException("denied", OMException.ResultCodes.PERMISSION_DENIED));
    HttpServletResponse response = doGet(om, "alice");

    verify(response).setStatus(HttpServletResponse.SC_FORBIDDEN);
    verify(response, never()).getWriter();
  }

  @Test
  public void testRequestWithoutUserWhenAclsEnabled() throws Exception {
    OzoneManager om = mockOzoneManager(true);
    HttpServletResponse response = doGet(om, null);

    verify(response).setStatus(HttpServletResponse.SC_FORBIDDEN);
    verify(om, never()).listSnapshot(any(), any(), any(), any(), anyInt());
  }

  @Test
  public void testRequestWithoutUserWhenAclsDisabled() throws Exception {
    OzoneManager om = mockOzoneManager(false);
    List<String> users = recordListSnapshotUsers(om);
    HttpServletResponse response = doGet(om, null);

    verify(response, never()).setStatus(anyInt());
    assertThat(users).containsExactly(UserGroupInformation.getCurrentUser().getShortUserName());
  }

  private static OzoneManager mockOzoneManager(boolean aclsEnabled) {
    OzoneManager om = mock(OzoneManager.class);
    when(om.getAclsEnabled()).thenReturn(aclsEnabled);
    return om;
  }

  /** @return the users that {@code om.listSnapshot} is called as. */
  private static List<String> recordListSnapshotUsers(OzoneManager om) throws Exception {
    List<String> users = new ArrayList<>();
    when(om.listSnapshot(eq("vol1"), eq("bucket1"), any(), any(), anyInt())).thenAnswer(invocation -> {
      users.add(UserGroupInformation.getCurrentUser().getShortUserName());
      return new ListSnapshotResponse(Collections.emptyList(), null);
    });
    return users;
  }

  private static HttpServletResponse doGet(OzoneManager om, String user) throws Exception {
    ServletContext context = mock(ServletContext.class);
    when(context.getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE)).thenReturn(om);
    ServletConfig config = mock(ServletConfig.class);
    when(config.getServletContext()).thenReturn(context);
    SnapshotListJSONServlet servlet = new SnapshotListJSONServlet();
    servlet.init(config);

    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getParameter("volume")).thenReturn("vol1");
    when(request.getParameter("bucket")).thenReturn("bucket1");
    when(request.getRemoteUser()).thenReturn(user);
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.getWriter()).thenReturn(new PrintWriter(new StringWriter()));
    servlet.doGet(request, response);
    return response;
  }
}
