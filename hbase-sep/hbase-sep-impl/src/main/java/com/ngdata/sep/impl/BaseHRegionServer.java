/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ngdata.sep.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;

import java.io.IOException;

/**
 */
public class BaseHRegionServer implements AdminProtos.AdminService.BlockingInterface, Server, org.apache.hadoop.hbase.ipc.PriorityFunction {
    @Override
    public Configuration getConfiguration() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ZKWatcher getZooKeeper() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ServerName getServerName() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void abort(String s, Throwable throwable) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean isAborted() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void stop(String s) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean isStopped() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public org.apache.hadoop.hbase.client.ClusterConnection getConnection() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Connection createConnection(Configuration configuration) throws IOException {
        return null;
    }

    @Override
    public ClusterConnection getClusterConnection() {
        return null;
    }

    @Override
    public org.apache.hadoop.hbase.zookeeper.MetaTableLocator getMetaTableLocator() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public org.apache.hadoop.hbase.CoordinatedStateManager getCoordinatedStateManager() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public org.apache.hadoop.hbase.ChoreService getChoreService() {
        throw new UnsupportedOperationException("Not implemented");
    }


    @Override
    public int getPriority(org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader requestHeader, org.apache.hbase.thirdparty.com.google.protobuf.Message message, User user) {
        return 0;
    }

    @Override
    public long getDeadline(org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader requestHeader, org.apache.hbase.thirdparty.com.google.protobuf.Message message) {
        return 0;
    }

    @Override
    public AdminProtos.GetRegionInfoResponse getRegionInfo(org.apache.hbase.thirdparty.com.google.protobuf.RpcController rpcController, AdminProtos.GetRegionInfoRequest getRegionInfoRequest) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public AdminProtos.GetStoreFileResponse getStoreFile(org.apache.hbase.thirdparty.com.google.protobuf.RpcController rpcController, AdminProtos.GetStoreFileRequest getStoreFileRequest) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public AdminProtos.GetOnlineRegionResponse getOnlineRegion(org.apache.hbase.thirdparty.com.google.protobuf.RpcController rpcController, AdminProtos.GetOnlineRegionRequest getOnlineRegionRequest) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public AdminProtos.OpenRegionResponse openRegion(org.apache.hbase.thirdparty.com.google.protobuf.RpcController rpcController, AdminProtos.OpenRegionRequest openRegionRequest) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public AdminProtos.WarmupRegionResponse warmupRegion(org.apache.hbase.thirdparty.com.google.protobuf.RpcController rpcController, AdminProtos.WarmupRegionRequest warmupRegionRequest) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public AdminProtos.CloseRegionResponse closeRegion(org.apache.hbase.thirdparty.com.google.protobuf.RpcController rpcController, AdminProtos.CloseRegionRequest closeRegionRequest) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public AdminProtos.FlushRegionResponse flushRegion(org.apache.hbase.thirdparty.com.google.protobuf.RpcController rpcController, AdminProtos.FlushRegionRequest flushRegionRequest) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public AdminProtos.CompactRegionResponse compactRegion(org.apache.hbase.thirdparty.com.google.protobuf.RpcController rpcController, AdminProtos.CompactRegionRequest compactRegionRequest) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public AdminProtos.ReplicateWALEntryResponse replicateWALEntry(org.apache.hbase.thirdparty.com.google.protobuf.RpcController rpcController, AdminProtos.ReplicateWALEntryRequest replicateWALEntryRequest) {
        return null;
    }

    @Override
    public AdminProtos.ReplicateWALEntryResponse replay(org.apache.hbase.thirdparty.com.google.protobuf.RpcController rpcController, AdminProtos.ReplicateWALEntryRequest replicateWALEntryRequest) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public AdminProtos.RollWALWriterResponse rollWALWriter(org.apache.hbase.thirdparty.com.google.protobuf.RpcController rpcController, AdminProtos.RollWALWriterRequest rollWALWriterRequest) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public AdminProtos.GetServerInfoResponse getServerInfo(org.apache.hbase.thirdparty.com.google.protobuf.RpcController rpcController, AdminProtos.GetServerInfoRequest getServerInfoRequest) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public AdminProtos.StopServerResponse stopServer(org.apache.hbase.thirdparty.com.google.protobuf.RpcController rpcController, AdminProtos.StopServerRequest stopServerRequest) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public AdminProtos.UpdateFavoredNodesResponse updateFavoredNodes(org.apache.hbase.thirdparty.com.google.protobuf.RpcController rpcController, AdminProtos.UpdateFavoredNodesRequest updateFavoredNodesRequest) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public AdminProtos.UpdateConfigurationResponse updateConfiguration(org.apache.hbase.thirdparty.com.google.protobuf.RpcController rpcController, AdminProtos.UpdateConfigurationRequest updateConfigurationRequest) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public AdminProtos.GetRegionLoadResponse getRegionLoad(org.apache.hbase.thirdparty.com.google.protobuf.RpcController rpcController, AdminProtos.GetRegionLoadRequest getRegionLoadRequest) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public AdminProtos.ClearCompactionQueuesResponse clearCompactionQueues(org.apache.hbase.thirdparty.com.google.protobuf.RpcController rpcController, AdminProtos.ClearCompactionQueuesRequest clearCompactionQueuesRequest) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public AdminProtos.ClearRegionBlockCacheResponse clearRegionBlockCache(org.apache.hbase.thirdparty.com.google.protobuf.RpcController rpcController, AdminProtos.ClearRegionBlockCacheRequest clearRegionBlockCacheRequest) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public QuotaProtos.GetSpaceQuotaSnapshotsResponse getSpaceQuotaSnapshots(org.apache.hbase.thirdparty.com.google.protobuf.RpcController rpcController, QuotaProtos.GetSpaceQuotaSnapshotsRequest getSpaceQuotaSnapshotsRequest) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public AdminProtos.ExecuteProceduresResponse executeProcedures(org.apache.hbase.thirdparty.com.google.protobuf.RpcController rpcController, AdminProtos.ExecuteProceduresRequest executeProceduresRequest) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
