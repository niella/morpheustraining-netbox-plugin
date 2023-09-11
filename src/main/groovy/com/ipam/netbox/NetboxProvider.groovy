/*
* Copyright 2022 the original author or authors.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.ipam.netbox

import com.morpheusdata.core.DNSProvider
import com.morpheusdata.core.IPAMProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.util.ConnectionUtils
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.core.util.NetworkUtility
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.AccountIntegration
import com.morpheusdata.model.Icon
import com.morpheusdata.model.NetworkDomain
import com.morpheusdata.model.NetworkDomainRecord
import com.morpheusdata.model.NetworkPool
import com.morpheusdata.model.NetworkPoolIp
import com.morpheusdata.model.NetworkPoolRange
import com.morpheusdata.model.NetworkPoolServer
import com.morpheusdata.model.NetworkPoolType
import com.morpheusdata.model.OptionType
import com.morpheusdata.model.projection.NetworkDomainIdentityProjection
import com.morpheusdata.model.projection.NetworkDomainRecordIdentityProjection
import com.morpheusdata.model.projection.NetworkPoolIdentityProjection
import com.morpheusdata.model.projection.NetworkPoolIpIdentityProjection
import com.morpheusdata.response.ServiceResponse
import groovy.util.logging.Slf4j
import io.reactivex.Single
import org.apache.commons.net.util.SubnetUtils
import org.apache.http.entity.ContentType
import io.reactivex.Observable

import java.nio.ByteBuffer

/**
 * The IPAM / DNS Provider implementation for Netbox
 * This contains most methods used for interacting directly with the Netbox 8.0+ REST API
 * 
 * @author Neil van Rensburg
 */
@Slf4j
class NetboxProvider implements IPAMProvider, DNSProvider {

    MorpheusContext morpheusContext
    Plugin plugin
    String authToken

    static final API_PATH_DNS_RESOURCE_RECORDS = "/api/plugins/netbox-dns/records/"
    static final API_PATH_DNS_ZONES = "/api/plugins/netbox-dns/zones/"
    static final API_PATH_SUBNET_PREFIXES = "/api/ipam/prefixes"
    static final API_PATH_IP_ADDRESSES = "/api/ipam/ip-addresses/"
    static final API_PATH_IP_RANGES = "/api/ipam/ip-ranges"
    static final RANGE_CONSTANT = 900000
    static DNS_PLUGIN_INSTALLED = null

    NetboxProvider(Plugin plugin, MorpheusContext morpheusContext) {
        this.morpheusContext = morpheusContext
        this.plugin = plugin
        this.authToken = null
    }


    /**
     * Creates a manually allocated DNS Record of the specified record type on the passed {@link NetworkDomainRecord} object.
     * This is typically called outside of automation and is a manual method for administration purposes.
     * @param integration The DNS Integration record which contains things like connectivity info to the DNS Provider
     * @param record The domain record that is being requested for creation. All the metadata needed to create teh record
     *               should exist here.
     * @param opts any additional options that may be used in the future to configure behavior. Currently unused
     * @return a ServiceResponse with the success/error state of the create operation as well as the modified record.
     */


    @Override
    public ServiceResponse createRecord(AccountIntegration integration, NetworkDomainRecord domainRecord, Map opts) {
        //println(">>CREATERECORD METHOD")
        ServiceResponse<NetworkDomainRecord> rtn = new ServiceResponse<>()
        HttpApiClient client = new HttpApiClient()
        def poolServer = morpheus.network.getPoolServerByAccountIntegration(integration).blockingGet()
        if (!DNS_PLUGIN_INSTALLED) {
            log.debug("DNS Plugin not active, skipping")
            rtn.success = true
            return rtn
        }

        try {
            if (!integration) {
                log.error("no integration when trying to create DNS Resource Record.")
                rtn.success = false
                return rtn
            }
            def fqdn = domainRecord.fqdn
            if (!domainRecord.name.endsWith(domainRecord.networkDomain.name)) {
                fqdn = "${domainRecord.name}.${domainRecord.networkDomain.name}"
            }
            def fqdnParts = fqdn.split("\\.")
            def fqdnDomain = fqdnParts[1..-1].join('.')
            def listResults = listDnsResourceRecords(client, poolServer, opts + [queryParams:[type: "A", zone: "${fqdnDomain}", name: "${domainRecord.name}"]])
            log.debug("RR results for $fqdn: " + listResults.data)

            if (!listResults.success) {
                log.error("Error querying Resource Records. Response Error: ${listResults.getErrorCode()}: $listResults")
            }
            if (listResults.success && listResults.data) {
                def existingRecord = listResults.data.first()
                domainRecord.externalId = existingRecord.id
                log.debug("Returning existing RR Record $domainRecord.name $domainRecord.content")
                return new ServiceResponse<NetworkDomainRecord>(true, null, null, domainRecord)
            } else {
                def zoneId = getZoneIdByName(fqdnDomain, client, poolServer, opts)
                authToken = authToken ?: getAuthToken(client, poolServer)
                def requestOpts = new HttpApiClient.RequestOptions(
                        headers: ['Content-Type': 'application/json','Authorization': "Token $authToken".toString()],
                        ignoreSSL: poolServer.ignoreSsl,
                        body: [
                                'zone': ['id': zoneId],
                                'type' : "A",
                                'name': domainRecord.name,
                                'status': "active",
                                'value': domainRecord.content.split("/")[0]
                        ]
                )
                log.debug("DNS Resource Record create POST body: ${requestOpts.body}")
                def results = client.callJsonApi(poolServer.serviceUrl, API_PATH_DNS_RESOURCE_RECORDS, null, null, requestOpts, 'POST')
                if (results.success) {
                    rtn.success = true
                    domainRecord.externalId = results.data?.id
                    return new ServiceResponse<NetworkDomainRecord>(true, null, null, domainRecord)
                } else {
                    rtn.success = false
                    log.error("Error creating Resource Record Response Error: ${results.getErrorCode()}: $results")
                }
            }
        } catch (e) {
            log.error("createRecord error: ${e}", e)
            throw new Exception(e)
        } finally {
            client.shutdownClient()
        }
        return rtn
    }


    /**
     * Deletes a Zone Record that is specified on the Morpheus side with the target integration endpoint.
     * This could be any record type within the specified integration and the authoritative zone object should be
     * associated with the {@link NetworkDomainRecord} parameter.
     * @param integration The DNS Integration record which contains things like connectivity info to the DNS Provider
     * @param record The zone record object to be deleted on the target integration.
     * @param opts opts any additional options that may be used in the future to configure behavior. Currently unused
     * @return the ServiceResponse with the success/error of the delete operation.
     */
    @Override
    ServiceResponse deleteRecord(AccountIntegration integration, NetworkDomainRecord record, Map opts) {
        //println(">>DELETERECORD METHOD")
        def rtn = new ServiceResponse()
        try {
            if (integration) {
                morpheus.network.getPoolServerByAccountIntegration(integration).doOnSuccess({ poolServer ->
                    if (!DNS_PLUGIN_INSTALLED) {
                        log.debug("DNS Plugin not active, skipping delete record...")
                        rtn.success = true
                        return rtn
                    }
                    HttpApiClient client = new HttpApiClient()
                    authToken = authToken ?: getAuthToken(client, poolServer)
                    try {
                        String apiPath = API_PATH_DNS_RESOURCE_RECORDS + "${record.externalId}/"
                        def requestOpts = new HttpApiClient.RequestOptions(
                                headers: ['Content-Type':'application/json','Authorization': "Token $authToken"],
                                ignoreSSL: poolServer.ignoreSsl,
                                contentType: ContentType.APPLICATION_JSON
                        )
                        rtn = client.callJsonApi(poolServer.serviceUrl, apiPath, null, null, requestOpts, 'DELETE')
                        log.debug("DNS resource record results: ${results.data}")
                        if (!rtn.success) {
                            log.error("Unable to delete DNS resource record with id ${record.externalId}: $results.data")
                        }
                    } finally {
                        client.shutdownClient()
                    }
                }).doOnError({error ->
                    log.error("Error deleting DNS resource record: {}", error.message,error)
                }).doOnSubscribe({ sub ->
                    log.info "Subscribed"
                }).blockingGet()
                return rtn
            } else {
                log.warn("DNS resource record: no integration")
                return rtn
            }
        } catch (e) {
            log.error("Delete DNS resource record error: ${e}", e)
            return rtn
        }
    }


    /**
     * Validation Method used to validate all inputs applied to the integration of an IPAM Provider upon save.
     * If an input fails validation or authentication information cannot be verified, Error messages should be returned
     * via a {@link ServiceResponse} object where the key on the error is the field name and the value is the error message.
     * If the error is a generic authentication error or unknown error, a standard message can also be sent back in the response.
     *
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     * @param opts any custom payload submission options may exist here
     * @return A response is returned depending on if the inputs are valid or not.
     */
    @Override
    ServiceResponse verifyNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
        //println(">>VERIFYNETWORKPOOLSERVER METHOD")
        HttpApiClient client = new HttpApiClient()
        authToken = authToken ?: getAuthToken(client, poolServer)

        def rtn = new ServiceResponse(success: true)
        def apiPath = "/api/status"
        Integer offset = 0

        HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(
                headers: ['Content-Type': 'application/json', 'Authorization': "Token $authToken".toString()],
                contentType: ContentType.APPLICATION_JSON,
                ignoreSSL: poolServer.ignoreSsl
        )
        log.debug("Checking Netbox API Status")
        ServiceResponse results = client.callJsonApi(poolServer.serviceUrl, apiPath, null, null, requestOpts, 'GET')
        log.debug("Status results: ${results.toString()}")

        if (!results?.data?.containsKey('rq-workers-running')) {
            log.error("Results object \"rq-workers-running\" missing from Netbox API status")
            rtn.success = false
        } else if (results?.data?.get('rq-workers-running') < 1) {
            log.error("Results object \"rq-workers-running\" missing from Netbox API status")
            rtn.success = false
        }
        return rtn
    }


    Boolean dnsPluginPresent(HttpApiClient client, NetworkPoolServer poolServer) {
        try {
            if (DNS_PLUGIN_INSTALLED == null) {
                ServiceResponse sr = listDnsResourceRecords(client, poolServer, [:])
                DNS_PLUGIN_INSTALLED = sr.success
            }
        } catch (Exception ex) {
            DNS_PLUGIN_INSTALLED = false
        }
        println("Netbox DNS Plugin present: $DNS_PLUGIN_INSTALLED")
        return DNS_PLUGIN_INSTALLED
    }


    ServiceResponse testNetworkPoolServer(HttpApiClient client, NetworkPoolServer poolServer) {
        //println(">>TESTNETWORKPOOLSERVER METHOD")
        def rtn = new ServiceResponse()
        try {
            def prefixListResponse = listIpAddresses(client, poolServer, [:])
            rtn.success = prefixListResponse.success
            rtn.data = [:]
            if (prefixListResponse.success) {
                dnsPluginPresent(client, poolServer)
            } else {
                rtn.msg = "Error connecting to Netbox API: ${prefixListResponse.getErrorCode()}: $prefixListResponse.data"
            }
        } catch(e) {
            rtn.success = false
            log.error("Test Netbox network pool server error: ${e}", e)
        }
        return rtn
    }


    /**
     * Called during creation of a {@link NetworkPoolServer} operation. This allows for any custom operations that need
     * to be performed outside of the standard operations.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     * @param opts any custom payload submission options may exist here
     * @return A response is returned depending on if the operation was a success or not.
     */
    @Override
    ServiceResponse createNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
        println(">>EMPTY CREATENETWORKPOOLSERVER METHOD")
        return null
    }


    /**
     * Called during update of an existing {@link NetworkPoolServer}. This allows for any custom operations that need
     * to be performed outside of the standard operations.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     * @param opts any custom payload submission options may exist here
     * @return A response is returned depending on if the operation was a success or not.
     */
    @Override
    ServiceResponse updateNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
        println(">>EMPTY UPDATENETWORKPOOLSERVER METHOD")
        return null
    }

    /**
     * Periodically called to refresh and sync data coming from the relevant integration. Most integration providers
     * provide a method like this that is called periodically (typically 5 - 10 minutes). DNS Sync operates on a 10min
     * cycle by default. Useful for caching Host Records created outside of Morpheus.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     */
    @Override
    void refresh(NetworkPoolServer poolServer) {
        println(">>REFRESH METHOD")

        HttpApiClient netboxClient = new HttpApiClient()
        def acc = poolServer.configMap
        log.info("refreshNetworkPoolServer: {}", poolServer.dump())

        //netboxClient.throttleRate = poolServer.serviceThrottleRate
        try {
            def apiUrlObj = new URL(poolServer.serviceUrl)
            def apiPort = apiUrlObj.port > 0 ? apiUrlObj.port : (apiUrlObj?.protocol?.toLowerCase() == 'https' ? 443 : 80)
            def hostOnline = ConnectionUtils.testHostConnectivity(apiUrlObj.host, apiPort, false, true, null)
            DNS_PLUGIN_INSTALLED = dnsPluginPresent(netboxClient, poolServer)
            log.debug("online: {} - {}", apiUrlObj.host, hostOnline)
            def testResults = new ServiceResponse()

            if (hostOnline) {
                testResults = testNetworkPoolServer(netboxClient,poolServer) as ServiceResponse<Map>
                if(!testResults.success) {
                    morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.error, 'error calling Netbox API').blockingGet()
                } else {
                    morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.syncing).blockingGet()
                }
            } else {
                morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.error, 'Netbox API not reachable')
            }
            Date now = new Date()
            if (testResults.success) {
                println("DNS Plugin Installed: $DNS_PLUGIN_INSTALLED")
                cacheNetworks(netboxClient, poolServer)
                if (DNS_PLUGIN_INSTALLED) {
                    cacheZones(netboxClient, poolServer)
                }

                if (poolServer?.configMap?.inventoryExisting) {
                    cacheIpAddressRecords(netboxClient, poolServer)
                    if (DNS_PLUGIN_INSTALLED) {
                        cacheZoneRecords(netboxClient, poolServer)
                    }
                }

                log.info("Sync Completed in ${new Date().time - now.time}ms")
                morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.ok).subscribe().dispose()
            }
        } catch (e) {
            log.error("refreshNetworkPoolServer error: ${e}", e)
        } finally {
            netboxClient.shutdownClient()
        }
    }





    // cacheNetworks methods
    void cacheNetworks(HttpApiClient client, NetworkPoolServer poolServer, Map opts = [:]) {
        println(">>CACHENETWORKS METHOD")
        opts.doPaging = true

        List apiItems = getNetworkPrefixesAndRanges(client, poolServer)

        if (apiItems) {
            log.debug("API Reocrds: ${apiItems}")
            Observable<NetworkPoolIdentityProjection> poolRecords = morpheus.network.pool.listIdentityProjections(poolServer.id)
            log.debug("Pool Reocrds: ${poolRecords}")

            SyncTask<NetworkPoolIdentityProjection, Map, NetworkPool> syncTask = new SyncTask(poolRecords, apiItems as Collection<Map>)
            syncTask.addMatchFunction { NetworkPoolIdentityProjection domainObject, Map apiItem ->

                //if (domainObject.externalId.toInteger() == apiItem.id.toInteger()) {
                //    println("FOUND NETWORK ${apiItem.isRange ? "RANGE" : "POOL"} MATCH: $domainObject.typeCode, $domainObject.externalId, (Id: $apiItem.id)")
                //    log.debug("FOUND NETWORK ${apiItem.isRange ? "RANGE" : "POOL"} MATCH: $domainObject.typeCode, $domainObject.externalId, (Id: $apiItem.id)")
                //}

                if (apiItem.isRange) {
                    domainObject.typeCode == 'netbox.range' && domainObject.externalId.toInteger() == apiItem.id.toInteger()
                } else if (apiItem.is_pool) {
                    domainObject.typeCode == 'netbox.pool' && domainObject.externalId.toInteger() == apiItem.id.toInteger()
                } else { //subnet
                    domainObject.typeCode == 'netbox.subnet' && domainObject.externalId.toInteger() == apiItem.id.toInteger()
                }
            }.onDelete {removeItems ->
                morpheus.network.pool.remove(poolServer.id, removeItems).blockingGet()
            }.onAdd { itemsToAdd ->
                addMissingPools(poolServer, itemsToAdd)
            }.withLoadObjectDetails { List<SyncTask.UpdateItemDto<NetworkPoolIdentityProjection,Map>> updateItems ->
                Map<Long, SyncTask.UpdateItemDto<NetworkPoolIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
                return morpheus.network.pool.listById(updateItems.collect{it.existingItem.id} as Collection<Long>).map { NetworkPool pool ->
                    SyncTask.UpdateItemDto<NetworkPoolIdentityProjection, Map> matchItem = updateItemMap[pool.id]
                    return new SyncTask.UpdateItem<NetworkPool,Map>(existingItem: pool, masterItem: matchItem.masterItem)
                }
            }.onUpdate { List<SyncTask.UpdateItem<NetworkPool,Map>> updateItems ->
                updateMatchedPools(poolServer, updateItems)
            }.start()
        }
    }


    void addMissingPools(NetworkPoolServer poolServer, Collection<Map> chunkedAddList) {
        println(">>ADDMISSINGPOOLS METHOD")

        List<NetworkPool> missingPoolsList = []

        chunkedAddList?.each { Map add ->
            def poolConfig = NetboxUtils.generateNetworkPoolConfig(poolServer.id, add)
            println("Adding Missing Pool: ${poolConfig.config.name}")
            //println("Pool details: $poolConfig")
            def newNetworkPool = new NetworkPool(poolConfig.config)
            newNetworkPool.ipRanges = []
            poolConfig.ranges.each { Map rangeConfig ->
                def addRange = new NetworkPoolRange(rangeConfig)
                newNetworkPool.ipRanges.add(addRange)
            }
            missingPoolsList.add(newNetworkPool)
        }
        morpheus.network.pool.create(poolServer.id, missingPoolsList).blockingGet()
    }


    void updateMatchedPools(NetworkPoolServer poolServer, List<SyncTask.UpdateItem<NetworkPool,Map>> chunkedUpdateList) {
        println("UPDATEMATCHEDPOOLS METHOD")

        List<NetworkPool> poolsToUpdate = []
        chunkedUpdateList?.each { update ->

            NetworkPool existingItem = update.existingItem
            //println("MasterItem: ${update.masterItem}")

            def masterItemConfig = NetboxUtils.generateNetworkPoolConfig(poolServer.id, update.masterItem)
            def masterItem = masterItemConfig.config
            def masterIpRanges = masterItemConfig.ranges

            if (existingItem) {
                def save = false
                def updateAttribute = { existingItemMap, attributeName, attributeValue ->
                    if (existingItemMap."$attributeName" != attributeValue) {
                        existingItemMap."$attributeName" = attributeValue
                        return true
                    }
                    return false
                }

                save = updateAttribute(existingItem, "ipCount", masterItem.ipCount) || save
                save = updateAttribute(existingItem, "name", masterItem.name) || save
                save = updateAttribute(existingItem, "displayName", masterItem.displayName) || save
                save = updateAttribute(existingItem, "poolEnabled", masterItem.poolEnabled) || save
                save = updateAttribute(existingItem, "parentType", "NetworkPoolServer") || save
                save = updateAttribute(existingItem, "type", masterItem.type) || save
                save = updateAttribute(existingItem, "configuration", masterItem.site?.id ?: 0) || save

                def rangeInList = { rangeItemMap, rangeList ->
                    def rtn = rangeList.any {range ->
                        if (range.startAddress == rangeItemMap.startAddress &&
                                range.endAddress == rangeItemMap.endAddress) {
                            return true
                        }
                        return false
                    }
                    //println(rtn ? "Found Existing Child Range: ${rangeItemMap.startAddress} - ${rangeItemMap.endAddress}" : "Child Range ${rangeItemMap.startAddress} - ${rangeItemMap.endAddress} not found")
                    return rtn
                }

                log.debug("Checking Master Ranges against existing, save is $save...")
                save = masterIpRanges.any { !rangeInList(it, existingItem.ipRanges) }
                if (!save) {
                    log.debug("Checking Existing Ranges against Master, save is $save...")
                    save = existingItem.ipRanges.any { !rangeInList(it, masterIpRanges) }
                }
                println("UPDATE EXISTING POOL $existingItem.name: $save")
                if (save) {
                    println("Saving, updating ranges...")
                    existingItem.ipRanges = []
                    println("Pool ${existingItem.name} for UPDATE with ${existingItem.ipRanges.size()} ranges, previously ${masterIpRanges.size()}:")
                    masterIpRanges.each { Map rangeConfig ->
                        def addRange = new NetworkPoolRange(rangeConfig)
                        println("Adding range ${rangeConfig.startAddress} - ${rangeConfig.endAddress}")
                        existingItem.addToIpRanges(addRange)
                    }
                    existingItem.ipRanges.each { range ->
                        println("${range.startAddress} - ${range.endAddress}")
                    }
                    poolsToUpdate << existingItem
                }
            }
        }
        if(poolsToUpdate.size() > 0) {
            log.debug("Updating ${poolsToUpdate.size()} pools")
            morpheus.network.pool.save(poolsToUpdate).blockingGet()
        } else {
            log.debug("No pools to update...")
        }
    }


    // cacheIpAddressRecord Methods
    void cacheIpAddressRecords(HttpApiClient client, NetworkPoolServer poolServer, Map opts=[:]) {
        println(">>CACHEIPADDRESSRECORDS METHOD")

        def getIPApiItemsInRange = { NetworkPool pool, List<Map> ipList ->
            def rtn = []
            def poolCidrLen = NetboxUtils.getPrefixLength(pool.cidr)
            ipList.each { ip ->
                pool.ipRanges.each { range ->
                    //println("FOUND RANGE ${range.startAddress}/$poolCidrLen - ${range.endAddress}/$poolCidrLen IN CIDR ${pool.cidr}. IP ${ip.address} Present: " + NetboxUtils.isIPInRange(ip.address, "${range.startAddress}/$poolCidrLen", "${range.endAddress}/$poolCidrLen"))
                    if (NetboxUtils.isIPInRange(ip.address, "${range.startAddress}/$poolCidrLen", "${range.endAddress}/$poolCidrLen")) {
                        rtn << ip
                    }
                }
            }
            return rtn
        }

        morpheus.network.pool.listIdentityProjections(poolServer.id).buffer(50).flatMap { Collection<NetworkPoolIdentityProjection> poolIdents ->
            return morpheus.network.pool.listById(poolIdents.collect{it.id})
        }.flatMap { NetworkPool pool ->
            def listResults
            listResults = listIpAddresses(client, poolServer,opts + [queryParams:[parent: "${pool.cidr}"]])
            if (listResults.success) {
                List<Map> apiItems = pool.type.getCode() == "netbox.range" ? getIPApiItemsInRange(pool, listResults.data) : listResults.data as List<Map>

                log.debug("Received these IPs: ${apiItems}")
                Observable<NetworkPoolIpIdentityProjection> poolIps = morpheus.network.pool.poolIp.listIdentityProjections(pool.id)
                SyncTask<NetworkPoolIpIdentityProjection, Map, NetworkPoolIp> syncTask = new SyncTask<NetworkPoolIpIdentityProjection, Map, NetworkPoolIp>(poolIps, apiItems)
                return syncTask.addMatchFunction { NetworkPoolIpIdentityProjection ipObject, Map apiItem ->
                    ipObject.externalId == apiItem.id
                }.addMatchFunction { NetworkPoolIpIdentityProjection domainObject, Map apiItem ->
                    def ipOnly = apiItem.address.toString().split("/")[0]
                    domainObject.ipAddress == ipOnly
                }.onDelete {removeItems ->
                    morpheus.network.pool.poolIp.remove(pool.id, removeItems).blockingGet()
                }.onAdd { itemsToAdd ->
                    addMissingIps(pool, itemsToAdd)
                }.withLoadObjectDetails { List<SyncTask.UpdateItemDto<NetworkPoolIpIdentityProjection,Map>> updateItems ->

                    Map<Long, SyncTask.UpdateItemDto<NetworkPoolIpIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
                    return morpheus.network.pool.poolIp.listById(updateItems.collect{it.existingItem.id} as Collection<Long>).map { NetworkPoolIp poolIp ->
                        SyncTask.UpdateItemDto<NetworkPoolIpIdentityProjection, Map> matchItem = updateItemMap[poolIp.id]
                        return new SyncTask.UpdateItem<NetworkPoolIp,Map>(existingItem:poolIp, masterItem:matchItem.masterItem)
                    }

                }.onUpdate { List<SyncTask.UpdateItem<NetworkDomain,Map>> updateItems ->
                    updateMatchedIps(updateItems)
                }.observe()
            } else {
                return Single.just(false)
            }
        }.doOnError{ e ->
            log.error("cacheIpRecords error: ${e}", e)
        }.subscribe()

    }

    void addMissingIps(NetworkPool pool, List addList) {
        //println(">>ADDMISSINGIPS METHOD")

        List<NetworkPoolIp> poolIpsToAdd = addList?.collect { it ->

            def ipType = 'assigned'
            def addr = it.address.toString().split("/")[0]
            log.debug("Adding IP $it.address ($it.dns_name) to pool $pool.externalId")
            def addConfig = [
                    networkPool: pool,
                    networkPoolRange: pool.ipRanges ? pool.ipRanges.first() : null,
                    ipType: ipType,
                    hostname: it.dns_name,
                    ipAddress: addr,
                    externalId: it.id]
            def newObj = new NetworkPoolIp(addConfig)
            return newObj

        }

        if(poolIpsToAdd.size() > 0) {
            try {
                log.debug("Adding ${poolIpsToAdd.size()} IP addresses into Morpheus")
                morpheus.network.pool.poolIp.create(pool, poolIpsToAdd).blockingGet()
            } catch (Exception e) {
                log.error("ERROR ADDING IPs INTO MORPHEUS: $e")
            }
        }
    }


    void updateMatchedIps(List<SyncTask.UpdateItem<NetworkPoolIp,Map>> updateList) {
        //println(">>UPDATEMATCHEDIPS METHOD")
        List<NetworkPoolIp> ipsToUpdate = []
        updateList?.each {  update ->
            NetworkPoolIp existingItem = update.existingItem

            if(existingItem) {
                def hostname = update.masterItem.dns_name
                def ipType = 'assigned'
                def save = false
                if(existingItem.ipType != ipType) {
                    existingItem.ipType = ipType
                    save = true
                }
                if(existingItem.hostname != hostname) {
                    existingItem.hostname = hostname
                    save = true
                }
                if(save) {
                    ipsToUpdate << existingItem
                }
            }
        }
        if(ipsToUpdate.size() > 0) {
            morpheus.network.pool.poolIp.save(ipsToUpdate)
        }
    }


    // Cache Zones methods
    def cacheZones(HttpApiClient client, NetworkPoolServer poolServer, Map opts = [:]) {
        //println(">>CACHEZONES METHOD")
        try {
            def listResults = listDnsZones(client, poolServer, opts)
            if (listResults.success) {
                List apiItems = listResults.data as List<Map>

                Observable<NetworkDomainIdentityProjection> domainRecords = morpheus.network.domain.listIdentityProjections(poolServer.integration.id)
                SyncTask<NetworkDomainIdentityProjection,Map,NetworkDomain> syncTask = new SyncTask(domainRecords, apiItems as Collection<Map>)

                syncTask.addMatchFunction { NetworkDomainIdentityProjection domainObject, Map apiItem ->
                    domainObject.externalId == apiItem.id
                }.onDelete {removeItems ->
                    morpheus.network.domain.remove(poolServer.integration.id, removeItems).blockingGet()
                }.onAdd { itemsToAdd ->
                    addMissingZones(poolServer, itemsToAdd)
                }.withLoadObjectDetails { List<SyncTask.UpdateItemDto<NetworkDomainIdentityProjection,Map>> updateItems ->
                    Map<Long, SyncTask.UpdateItemDto<NetworkDomainIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
                    return morpheus.network.domain.listById(updateItems.collect{it.existingItem.id} as Collection<Long>).map { NetworkDomain networkDomain ->
                        SyncTask.UpdateItemDto<NetworkDomainIdentityProjection, Map> matchItem = updateItemMap[networkDomain.id]
                        return new SyncTask.UpdateItem<NetworkDomain,Map>(existingItem:networkDomain, masterItem:matchItem.masterItem)
                    }
                }.onUpdate { List<SyncTask.UpdateItem<NetworkDomain,Map>> updateItems ->
                    updateMatchedZones(poolServer, updateItems)
                }.start()

            }
        } catch (e) {
            log.error("cacheZones error: ${e}", e)
        }
    }

    /**
     * Creates a mapping for networkDomainService.createSyncedNetworkDomain() method on the network context.
     * @param poolServer
     * @param addList
     */
    void addMissingZones(NetworkPoolServer poolServer, Collection addList) {
        //println(">>ADDMISSINGZONES METHOD")
        List<NetworkDomain> missingZonesList = addList?.collect { Map add ->
            NetworkDomain networkDomain = new NetworkDomain()
            networkDomain.externalId = add.id
            networkDomain.name = NetworkUtility.getFriendlyDomainName(add.name as String)
            networkDomain.fqdn = NetworkUtility.getFqdnDomainName(add.name as String)
            networkDomain.refSource = 'integration'
            networkDomain.zoneType = 'Authoritative'
            networkDomain.configuration = add.id
            return networkDomain
        }
        morpheus.network.domain.create(poolServer.integration.id, missingZonesList).blockingGet()
    }

    /**
     * Given a pool server and updateList, extract externalId's and names to match on and update NetworkDomains.
     * @param poolServer
     * @param addList
     */
    void updateMatchedZones(NetworkPoolServer poolServer, List<SyncTask.UpdateItem<NetworkDomain,Map>> updateList) {
        //println(">>UPDATEMATCHEDZONES METHOD")
        def domainsToUpdate = []
        for(SyncTask.UpdateItem<NetworkDomain,Map> update in updateList) {
            NetworkDomain existingItem = update.existingItem as NetworkDomain
            if(existingItem) {
                Boolean save = false
                if(!existingItem.externalId) {
                    existingItem.externalId = update.masterItem.id
                    save = true
                }
                if(!existingItem.configuration) {
                    existingItem.configuration = update.masterItem.id
                    save = true
                }
                if(!existingItem.refId) {
                    existingItem.refType = 'AccountIntegration'
                    existingItem.refId = poolServer.integration.id
                    existingItem.refSource = 'integration'
                    save = true
                }

                if(save) {
                    domainsToUpdate.add(existingItem)
                }
            }
        }
        if(domainsToUpdate.size() > 0) {
            morpheus.network.domain.save(domainsToUpdate).blockingGet()
        }
    }


    // Cache Zones methods
    def cacheZoneRecords(HttpApiClient client, NetworkPoolServer poolServer, Map opts=[:]) {
        //println(">>CACHEZONERECORDS METHOD")

        morpheus.network.domain.listIdentityProjections(poolServer.integration.id).buffer(50).flatMap { Collection<NetworkDomainIdentityProjection> poolIdents ->
            return morpheus.network.domain.listById(poolIdents.collect{it.id})
        }.flatMap { NetworkDomain domain ->

            def listResults = listDnsResourceRecords(client,poolServer,opts + [queryParams:["id": "${domain.externalId}".toString()]])
            if (listResults.success) {
                List<Map> apiItems = listResults.data as List<Map>
                Observable<NetworkDomainRecordIdentityProjection> domainRecords = morpheus.network.domain.record.listIdentityProjections(domain, null)
                SyncTask<NetworkDomainRecordIdentityProjection, Map, NetworkDomainRecord> syncTask = new SyncTask<NetworkDomainRecordIdentityProjection, Map, NetworkDomainRecord>(domainRecords, apiItems)

                return syncTask.addMatchFunction {  NetworkDomainRecordIdentityProjection domainObject, Map apiItem ->
                    domainObject.externalId == apiItem.id
                }.onDelete {removeItems ->
                    morpheus.network.domain.record.remove(domain, removeItems).blockingGet()
                }.onAdd { itemsToAdd ->
                    addMissingDomainRecords(domain, itemsToAdd)
                }.withLoadObjectDetails { List<SyncTask.UpdateItemDto<NetworkDomainRecordIdentityProjection,Map>> updateItems ->

                    Map<Long, SyncTask.UpdateItemDto<NetworkDomainRecordIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
                    return morpheus.network.domain.record.listById(updateItems.collect{it.existingItem.id} as Collection<Long>).map { NetworkDomainRecord domainRecord ->
                        SyncTask.UpdateItemDto<NetworkDomainRecordIdentityProjection, Map> matchItem = updateItemMap[domainRecord.id]
                        return new SyncTask.UpdateItem<NetworkDomainRecord,Map>(existingItem:domainRecord, masterItem:matchItem.masterItem)
                    }

                }.onUpdate { List<SyncTask.UpdateItem<NetworkDomainRecord,Map>> updateItems ->
                    updateMatchedDomainRecords(updateItems)
                }.observe()

            } else {
                return Single.just(false)
            }

        }.doOnError{ e ->
            log.error("cacheIpRecords error: ${e}", e)
        }.subscribe()

    }


    void updateMatchedDomainRecords(List<SyncTask.UpdateItem<NetworkDomainRecord, Map>> updateList) {
        //println(">>UPDATEMATCHEDDOMAINRECORDS METHOD")
        def records = []
        updateList?.each { update ->
            NetworkDomainRecord existingItem = update.existingItem
            String recordType = update.masterItem.rr_type
            if(existingItem) {
                //update view ?
                def save = false
                if(update.masterItem.rr_all_value != existingItem.content) {
                    existingItem.setContent(update.masterItem.rr_all_value as String)
                    save = true
                }

                if(update.masterItem.rr_full_name_utf != existingItem.name) {
                    existingItem.name = update.masterItem.rr_full_name_utf
                    existingItem.fqdn = update.masterItem.rr_full_name_utf
                    save = true
                }

                if(save) {
                    records.add(existingItem)
                }
            }
        }
        if(records.size() > 0) {
            morpheus.network.domain.record.save(records).blockingGet()
        }
    }

    void addMissingDomainRecords(NetworkDomainIdentityProjection domain, Collection<Map> addList) {
        //println(">>ADDMISSINGDOMAINRECORDS METHOD")
        List<NetworkDomainRecord> records = []
        def zone = new NetworkDomain(id: domain.id)
        addList?.each {
            def addConfig = [
                    networkDomain: zone,
                    externalId: it.id,
                    name: it.name,
                    fqdn: "$it.name.$it.zone.name",
                    type: it.type,
                    source: 'sync']

            def newObj = new NetworkDomainRecord(addConfig)
            newObj.ttl = it.ttl?.toInteger()
            newObj.setContent(it.value.toString())
            records.add(newObj)
        }
        morpheus.network.domain.record.create(domain,records).blockingGet()
    }


    /**
     * Called on the first save / update of a pool server integration. Used to do any initialization of a new integration
     * Often times this calls the periodic refresh method directly.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     * @param opts an optional map of parameters that could be sent. This may not currently be used and can be assumed blank
     * @return a ServiceResponse containing the success state of the initialization phase
     */
    @Override
    ServiceResponse initializeNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
        //println(">>INITIALIZENETWORKPOOLSERVER METHOD")
        def rtn = new ServiceResponse()
        try {
            if(poolServer) {
                refresh(poolServer)
                rtn.data = poolServer
            } else {
                rtn.error = 'No pool server found'
            }
        } catch(e) {
            rtn.error = "initializeNetworkPoolServer error: ${e}"
            log.error("initializeNetworkPoolServer error: ${e}", e)
        }
        return rtn
    }

    /**
     * Creates a Host record on the target {@link NetworkPool} within the {@link NetworkPoolServer} integration.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     * @param networkPool the NetworkPool currently being operated on.
     * @param networkPoolIp The ip address and metadata related to it for allocation. It is important to create functionality such that
     *                      if the ip address property is blank on this record, auto allocation should be performed and this object along with the new
     *                      ip address be returned in the {@link ServiceResponse}
     * @param domain The domain with which we optionally want to create an A/PTR record for during this creation process.
     * @param createARecord configures whether or not the A record is automatically created
     * @param createPtrRecord configures whether or not the PTR record is automatically created
     * @return a ServiceResponse containing the success state of the create host record operation
     */
    @Override
    ServiceResponse createHostRecord(NetworkPoolServer poolServer, NetworkPool networkPool, NetworkPoolIp networkPoolIp, NetworkDomain domain, Boolean createARecord, Boolean createPtrRecord) {
        //println(">>CREATEHOSTRECORD METHOD")
        HttpApiClient client = new HttpApiClient();
        authToken = authToken ?: getAuthToken(client, poolServer)
        try {
            String serviceUrl = poolServer.serviceUrl

            def hostname = networkPoolIp.hostname
            if (domain && hostname && !hostname.endsWith(domain.name)) {
                hostname = "${hostname}.${domain.name}"
            }

            def prefix = networkPool.cidr.split("/")[1]
            ServiceResponse reserveIpResults
            if(networkPoolIp.ipAddress) {

                def requestOpts = new HttpApiClient.RequestOptions(
                        headers: ['Content-Type': 'application/json','Authorization': "Token $authToken"],
                        ignoreSSL: poolServer.ignoreSsl,
                        body: [
                                'address': "$networkPoolIp.ipAddress/$prefix",
                                'dns_name':  hostname.toString(),
                                'status': "active"
                        ]
                )
                log.debug("Reserving IP with $networkPoolIp.ipAddress at $poolServer.serviceUrl$API_PATH_IP_ADDRESSES}")
                reserveIpResults = client.callJsonApi(poolServer.serviceUrl, API_PATH_IP_ADDRESSES, null, null, requestOpts, 'POST')
                log.debug("Reserve IP Results: ${reserveIpResults}")

            } else {

                def requestOpts = new HttpApiClient.RequestOptions(
                        headers: ['Content-Type': 'application/json','Authorization': "Token $authToken".toString()],
                        ignoreSSL: poolServer.ignoreSsl,
                        body: [
                                'dns_name':  hostname.toString(),
                                'status': "active"
                        ]
                )
                log.debug("Reserving without IP AT $serviceUrl, $API_PATH_SUBNET_PREFIXES/$networkPool.externalId/available-ips")
                reserveIpResults = client.callJsonApi(serviceUrl, "$API_PATH_SUBNET_PREFIXES/$networkPool.externalId/available-ips/", null, null, requestOpts, 'POST')
                log.debug("Reserve IP Results: ${reserveIpResults}")


            }

            if(reserveIpResults?.success) {
                def addrOnly = reserveIpResults.data.address.toString().split("/")[0]

                networkPoolIp.externalId = reserveIpResults.data.id
                networkPoolIp.ipAddress = addrOnly
                NetworkPoolRange range = new NetworkPoolRange()
                range.startAddress = networkPool.ipRanges.first().startAddress
                range.externalId = networkPool.ipRanges.first().externalId
                range.id = networkPool.ipRanges.first().id
                range.endAddress = networkPool.ipRanges.first().endAddress
                range.addressCount = networkPool.ipRanges.first().addressCount
                networkPoolIp.networkPoolRange = range ? range : null

                if(createARecord) {
                    networkPoolIp.domain = domain
                }
                if (networkPoolIp.id) {
                    networkPoolIp = morpheus.network.pool.poolIp.save(networkPoolIp)?.blockingGet()
                } else {
                    networkPoolIp = morpheus.network.pool.poolIp.create(networkPoolIp)?.blockingGet()
                }
                if (createARecord && domain) {
                    def domainRecord = new NetworkDomainRecord(
                            networkDomain: domain,
                            ttl: 3600,
                            networkPoolIp: networkPoolIp,
                            name: hostname,
                            fqdn: hostname,
                            source: 'user',
                            type: 'A',
                            content: networkPoolIp.ipAddress
                    )

                    def createRecordResults = createRecord(poolServer.integration, domainRecord,[:])
                    if(createRecordResults.success) {
                        if (networkPool.getConfigProperty("dnsPlugin")) {
                            log.debug("DNS Plugin not active, skipping")
                        } else {
                            morpheus.network.domain.record.create(domainRecord).blockingGet()
                        }
                    }
                }

                return ServiceResponse.success(networkPoolIp)
            } else {
                return ServiceResponse.error("Error allocating host record to the specified ip", null, networkPoolIp)
            }
        } finally {
            client.shutdownClient()
        }
    }

    /**
     * Updates a Host record on the target {@link NetworkPool} if supported by the Provider. If not supported, send the appropriate
     * {@link ServiceResponse} such that the user is properly informed of the unavailable operation.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     * @param networkPool the NetworkPool currently being operated on.
     * @param networkPoolIp the changes to the network pool ip address that would like to be made. Most often this is just the host record name.
     * @return a ServiceResponse containing the success state of the update host record operation
     */
    @Override
    ServiceResponse updateHostRecord(NetworkPoolServer poolServer, NetworkPool networkPool, NetworkPoolIp networkPoolIp) {
        //println(">>UPDATEHOSTRECORD METHOD")
        HttpApiClient client = new HttpApiClient();
        authToken = authToken ?: getAuthToken(client, poolServer)
        try {
            def prefix = networkPool.name.split("/")[1]
            def hostResults = listIpAddresses(client, poolServer, [queryParams: ['address': "$networkPoolIp.ipAddress/$prefix"]])
            def redId = hostResults.data.first().id
            if (!hostResults.data.first()) {
                log.error("Existing host record for $networkPoolIp.ipAddress/$prefix not found.")
            } else if (networkPoolIp.externalId != redId) {
                log.error("Found IP record ${redId} and current id $networkPoolIp.externalId don't match.")
                return ServiceResponse.error("Found IP record ${redId} and current id $networkPoolIp.externalId don't match.", null, networkPoolIp)
            } else {

                def requestOpts = new HttpApiClient.RequestOptions(
                        headers: ['Content-Type': 'application/json', 'Authorization': "Token $authToken".toString()],
                        ignoreSSL: poolServer.ignoreSsl,
                        body: [
                                'address' : "$networkPoolIp.ipAddress/$prefix",
                                'dns_name': networkPoolIp.hostname.toString(),
                                'status'  : "active"
                        ]
                )
                def ipAddResults = client.callJsonApi(poolServer.serviceUrl, "$API_PATH_IP_ADDRESSES/$networkPoolIp.externalId", null, null, requestOpts, 'PUT')

                if (ipAddResults?.success) {
                    def ipId = ipAddResults.data?.first()?.id
                    networkPoolIp.externalId = ipId
                    networkPoolIp.ipAddress = networkPoolIp.ipAddress
                    networkPoolIp.hostname = networkPoolIp.hostname
                    if (networkPoolIp.id) {
                        networkPoolIp = morpheus.network.pool.poolIp.save(networkPoolIp)?.blockingGet()
                    }
                    return ServiceResponse.success(networkPoolIp)
                } else {
                    return ServiceResponse.error("Error allocating host record to the specified ip", null, networkPoolIp)
                }
            }
        } finally {
            client.shutdownClient()
        }
    }

    /**
     * Deletes a host record on the target {@link NetworkPool}. This is used for cleanup or releasing of an ip address on
     * the IPAM Provider.
     * @param networkPool the NetworkPool currently being operated on.
     * @param poolIp the record that is being deleted.
     * @param deleteAssociatedRecords determines if associated records like A/PTR records
     * @return a ServiceResponse containing the success state of the delete operation
     */
    @Override
    ServiceResponse deleteHostRecord(NetworkPool networkPool, NetworkPoolIp poolIp, Boolean deleteAssociatedRecords) {
        //println(">>DELETEHOSTRECORD METHOD")
        HttpApiClient client = new HttpApiClient();
        def poolServer = morpheus.network.getPoolServerById(networkPool.poolServer.id).blockingGet()
        authToken = authToken ?: getAuthToken(client, poolServer)
        try {
            if(poolIp.externalId) {
                def requestOpts = new HttpApiClient.RequestOptions(
                        headers: ['Content-Type':'application/json', 'Authorization': "Token $authToken".toString()],
                        ignoreSSL: poolServer.ignoreSsl)
                def results = client.callApi(poolServer.serviceUrl, "$API_PATH_IP_ADDRESSES/$poolIp.externalId/", null, null, requestOpts, 'DELETE')
                if(results.success) {
                    return ServiceResponse.success(poolIp)
                } else {
                    return ServiceResponse.error(results.error ?: 'Error Deleting Host Record', null, poolIp)
                }
            } else {
                return ServiceResponse.error("Record not associated with corresponding record in target provider", null, poolIp)
            }
        } finally {
            client.shutdownClient()
        }
        return null
    }

    /**
     * Periodically called to refresh and sync data coming from the relevant integration. Most integration providers
     * provide a method like this that is called periodically (typically 5 - 10 minutes). DNS Sync operates on a 10min
     * cycle by default. Useful for caching DNS Records created outside of Morpheus.
     * NOTE: This method is unused when paired with a DNS Provider so simply return null
     * @param integration The Integration Object contains all the saved information regarding configuration of the DNS Provider.
     */
    @Override
    void refresh(AccountIntegration integration) {
        //println(">>REFRESH METHOD")
        //NOOP
    }

    /**
     * Validation Method used to validate all inputs applied to the integration of an DNS Provider upon save.
     * If an input fails validation or authentication information cannot be verified, Error messages should be returned
     * via a {@link ServiceResponse} object where the key on the error is the field name and the value is the error message.
     * If the error is a generic authentication error or unknown error, a standard message can also be sent back in the response.
     * NOTE: This is unused when paired with an IPAMProvider interface
     * @param integration The Integration Object contains all the saved information regarding configuration of the DNS Provider.
     * @param opts any custom payload submission options may exist here
     * @return A response is returned depending on if the inputs are valid or not.
     */
    @Override
    ServiceResponse verifyAccountIntegration(AccountIntegration integration, Map opts) {
        //println(">>VERIFYACCOUNTINTEGRATION METHOD")
        //NOOP

        return null
    }

    /**
     * An IPAM Provider can register pool types for display and capability information when syncing IPAM Pools
     * @return a List of {@link NetworkPoolType} to be loaded into the Morpheus database.
     */
    @Override
    Collection<NetworkPoolType> getNetworkPoolTypes() {
        //println(">>GETNETWORKPOOLTYPES METHOD")
        return [
                new NetworkPoolType(code: 'netbox.subnet', name:' Netbox Subnet', creatable: false, description:'Netbox Subnet', rangeSupportsCidr: true),
                new NetworkPoolType(code: 'netbox.pool', name: 'Netbox Subnet Pool', creatable: false, description:'Netbox Subnet Pool', rangeSupportsCidr: true),
                new NetworkPoolType(code: 'netbox.range', name: 'Netbox Range', creatable: false, description:'Netbox Rage', rangeSupportsCidr: true)
        ]
    }

    /**
     * Provide custom configuration options when creating a new {@link AccountIntegration}
     * @return a List of OptionType
     */
    @Override
    List<OptionType> getIntegrationOptionTypes() {
        //println(">>GETINTEGRATIONOPTIONTYPES METHOD")
        return [
                new OptionType(code: 'netbox.serviceUrl', name: 'Service URL', inputType: OptionType.InputType.TEXT, defaultValue: "", fieldName: 'serviceUrl', fieldLabel: 'API Url', fieldContext: 'domain', displayOrder: 0),
                new OptionType(code: 'netbox.credentials', name: 'Credentials', inputType: OptionType.InputType.CREDENTIAL, fieldName: 'type', fieldLabel: 'Credentials', fieldContext: 'credential', required: true, displayOrder: 1, defaultValue: 'local',optionSource: 'credentials',config: '{"credentialTypes":["username-password"]}'),
                new OptionType(code: 'netbox.serviceUsername', name: 'Service Username', inputType: OptionType.InputType.TEXT, defaultValue: "", fieldName: 'serviceUsername', fieldLabel: 'Username', fieldContext: 'domain', displayOrder: 2, localCredential: true),
                new OptionType(code: 'netbox.servicePassword', name: 'Service Password', inputType: OptionType.InputType.PASSWORD, defaultValue: "", fieldName: 'servicePassword', fieldLabel: 'Password', fieldContext: 'domain', displayOrder: 3, localCredential: true),
                new OptionType(code: 'netbox.throttleRate', name: 'Throttle Rate', inputType: OptionType.InputType.NUMBER, defaultValue: 0, fieldName: 'serviceThrottleRate', fieldLabel: 'Throttle Rate', fieldContext: 'domain', displayOrder: 4),
                new OptionType(code: 'netbox.ignoreSsl', name: 'Ignore SSL', inputType: OptionType.InputType.CHECKBOX, defaultValue: 1, fieldName: 'ignoreSsl', fieldLabel: 'Disable SSL SNI Verification', fieldContext: 'domain', displayOrder: 5),
                new OptionType(code: 'netbox.inventoryExisting', name: 'Inventory Existing', inputType: OptionType.InputType.CHECKBOX, defaultValue: 1, fieldName: 'inventoryExisting', fieldLabel: 'Inventory Existing', fieldContext: 'config', displayOrder: 6)
                //new OptionType(code: 'netbox.dnsPlugin', name: 'DNS Plugin Installed', inputType: OptionType.InputType.CHECKBOX, defaultValue: 0, fieldName: 'dnsPlugin', fieldLabel: 'Netbox DNS plugin installed on Netbox', fieldContext: 'domain', displayOrder: 7),
        ]
    }

    /**
     * Returns the IPAM Integration logo for display when a user needs to view or add this integration
     * @since 0.12.3
     * @return Icon representation of assets stored in the src/assets of the project.
     */
    @Override
    Icon getIcon() {
        return new Icon(path:"netbox-logo.svg", darkPath: "netbox-logo.svg")
    }

    /**
     * Returns the Morpheus Context for interacting with data stored in the Main Morpheus Application
     *
     * @return an implementation of the MorpheusContext for running Future based rxJava queries
     */
    @Override
    MorpheusContext getMorpheus() {
        return morpheusContext
    }

    /**
     * Returns the instance of the Plugin class that this provider is loaded from
     * @return Plugin class contains references to other providers
     */
    @Override
    Plugin getPlugin() {
        return plugin
    }

    /**
     * A unique shortcode used for referencing the provided provider. Make sure this is going to be unique as any data
     * that is seeded or generated related to this provider will reference it by this code.
     * @return short code string that should be unique across all other plugin implementations.
     */
    @Override
    String getCode() {
        return "netbox"
    }

    /**
     * Provides the provider name for reference when adding to the Morpheus Orchestrator
     * NOTE: This may be useful to set as an i18n key for UI reference and localization support.
     *
     * @return either an English name of a Provider or an i18n based key that can be scanned for in a properties file.
     */
    @Override
    String getName() {
        return "Netbox"
    }


    List<Map> getNetworkPrefixesAndRanges(HttpApiClient client, NetworkPoolServer poolServer) {
        def rtn = []
        def opts = [doPaging: true]

        def networkListResults = listNetworkSubnetsAndPools(client, poolServer, opts)
        log.debug("Network results: ${networkListResults}")

        def rangesListResults = listIPRanges(client, poolServer, opts)
        log.debug("Ranges found: ${rangesListResults}")

        rtn = networkListResults.data as List<Map>
        if (!networkListResults.success || !rangesListResults.success) {
            return rtn
        }

        for (Map range : (rangesListResults.data as List<Map>)) {
            def rangeHasParent = false
            range.isRange = true

            if (range.containsKey("parentPrefixId")) {
                for (Map prefix : (networkListResults.data as List<Map>)) {
                    if (range.parentPrefixId.toInteger() == prefix.id.toInteger()) {
                        rangeHasParent = true
                        if (prefix.containsKey("childRanges")) {
                            prefix.childRanges += range
                        } else {
                            prefix.childRanges = [range]
                        }
                    }
                }
            }
            //if (!rangeHasParent) {
            println("Adding range: $range")
                rtn << range
            //}
        }
        //println("returning: $rtn")
        return rtn
    }

    private ServiceResponse listNetworkSubnets(HttpApiClient client, NetworkPoolServer poolServer, Map opts) {
        listObjects("ip_block_subnet_list",client,poolServer,opts)
    }

    private ServiceResponse listPrefixes(HttpApiClient client, NetworkPoolServer poolServer, Map opts) {
        listObjects("ipam/prefixes",client,poolServer,opts)
    }

    private ServiceResponse listAvailableIpAddresses(long prefixId, HttpApiClient client, NetworkPoolServer poolServer, Map opts) {
        listObjects("$API_PATH_SUBNET_PREFIXES/$prefixId/available-ips/", client, poolServer, opts)
    }

    private ServiceResponse listIpAddresses(HttpApiClient client, NetworkPoolServer poolServer, Map opts) {
        listObjects(API_PATH_IP_ADDRESSES, client, poolServer, opts)
    }

    private ServiceResponse listDnsZones(HttpApiClient client, NetworkPoolServer poolServer, Map opts) {
        listObjects(API_PATH_DNS_ZONES, client, poolServer, opts)
    }

    private ServiceResponse listDnsResourceRecords(HttpApiClient client, NetworkPoolServer poolServer, Map opts) {

        listObjects(API_PATH_DNS_RESOURCE_RECORDS, client, poolServer, opts)
    }

    private ServiceResponse listNetworkSubnetsAndPools(HttpApiClient client, NetworkPoolServer poolServer, Map opts) {
        println(">>LISTNETWORKSUBNETSANDPOOLS METHOD")
        ServiceResponse response = new ServiceResponse(success: false)
        def networkList = listObjects(API_PATH_SUBNET_PREFIXES, client, poolServer, opts)
        if(networkList.success) {
            networkList.data.each { it.put('typeCode', it.is_pool ? 'netbox.pool' : 'netbox.subnet') }
            return networkList
        }
        return response
    }


    private ServiceResponse listIPRanges(HttpApiClient client, NetworkPoolServer poolServer, Map opts) {
        println(">>LISTIPRANGES METHOD")
        ServiceResponse response = new ServiceResponse(success: false)
        def rangesList = listObjects(API_PATH_IP_RANGES, client, poolServer, opts)
        if(rangesList.success) {
            for (range in rangesList.data) {
                range.typeCode = 'netbox.range'
                range.id = (int) range.id + RANGE_CONSTANT
                def netId = NetboxUtils.getNetworkPrefix(range.start_address)
                def netIdResult = listObjects(API_PATH_SUBNET_PREFIXES, client, poolServer, [queryParams: [prefix: "$netId"]])
                if (netIdResult.success) {
                    if (netIdResult.data) {
                        log.debug("Range starting at $range.start_address: Found parent prefix with id ${netIdResult.data.get(0).id}")
                        range.parentPrefixId = netIdResult.data.get(0).id
                    }
                }
            }
            return rangesList
        }
        return response
    }


    private ServiceResponse getPrefixId(HttpApiClient client, Map opts) {
        println(">>GETPREFIXID METHOD")
        ServiceResponse response = new ServiceResponse(success: false)
        def networkList = listObjects(API_PATH_SUBNET_PREFIXES, client, poolServer, opts)
        if(networkList.success) {
            return networkList
        }
        return response
    }


    private long getZoneIdByName(String zoneName, HttpApiClient client, NetworkPoolServer poolServer, Map opts) {
        //println(">>GETZONEIDBYNAME METHOD")
        def results = listObjects(API_PATH_DNS_ZONES, client, poolServer, opts +  [queryParams:[name: "$zoneName"]])
        if (results.data.size == 0) {
            log.error("Zone \"$zoneName\" not found.")
        } else if (results.data.size > 1) {
            log.error("Multiple Zones returned for name $zoneName")
        }

        return results.data.first().id as long
    }





    /**
     * Standard method for calling Netbox *_list REST Services. This auto pages the dataset in smaller chunks and returns all results
     * @param listService the service name from the Netbox REST API
     * @param client the HttpClient being used during this operation for keep-alive
     * @param poolServer the PoolServer integration we are referencing for credentials and connectivity info
     * @param opts other options
     * @return
     */
    private ServiceResponse listObjects(String apiPath, HttpApiClient client, NetworkPoolServer poolServer, Map opts) {
        //println(">>LISTOBJECTS METHOD")
        authToken = authToken ?: getAuthToken(client, poolServer)

        def rtn = new ServiceResponse(success: false)
        def hasMore = true
        def maxResults = opts.maxResults ?: 100
        rtn.data = []
        Integer offset = 0
        def attempt = 0

        HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(
                headers:['Content-Type':'application/json','Authorization': "Token $authToken".toString()],
                contentType: ContentType.APPLICATION_JSON,
                ignoreSSL: poolServer.ignoreSsl
        )

        while (hasMore && attempt < 1000) {
            attempt++
            log.debug("Fething results page $attempt...")
            Map<String,String> pageQuery = [limit:maxResults.toString(), offset:offset.toString()] + (opts?.queryParams?: [:])
            log.debug("Page query string: $pageQuery")
            requestOpts.queryParams = pageQuery
            ServiceResponse results = client.callJsonApi(poolServer.serviceUrl, apiPath,null, null, requestOpts, 'GET')
            log.debug("listNetworkSubnets results: ${results.toString()}")
            if (!results?.success || results?.hasErrors()) {
                log.error("Unable to request list from Netbox API at $apiPath: " + results.toString())
                rtn.headers = results.headers
                rtn.success = false
                return rtn
            }

            if (!results?.data?.containsKey('results')) {
                log.error("Results object missing from Netbox API at $apiPath: " + results.toString())
            }

            rtn.success = true
            rtn.headers = results.headers
            log.debug("Page $attempt results: " + results.data)
            def pageResults = results.data.get("results")
            if(pageResults?.size() > 0) {
                offset += pageResults?.size()
                rtn.data += pageResults
            } else {
                hasMore = false
            }
        }
        log.debug("Query results over $attempt pages:" + rtn.data)
        return rtn
    }


    String getAuthToken(HttpApiClient client, NetworkPoolServer poolServer) {
        def apiPath = "/api/users/tokens/provision/"
        HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(
                headers:['Content-Type':'application/json'],
                body:[
                        'username': poolServer.credentialData?.username as String ?: poolServer.serviceUsername,
                        'password': poolServer.credentialData?.password as String ?: poolServer.servicePassword
                ],
                contentType: ContentType.APPLICATION_JSON,
                ignoreSSL: poolServer.ignoreSsl
        )
        try {
            def results = client.callJsonApi(poolServer.serviceUrl, apiPath, null, null, requestOpts,  'POST')
            if (!results?.success || results?.hasErrors()) {
                log.error("Unable to request new Netbox API token: " + results.toString())
                throw new Exception("Unable to request new Netbox API token: " + results.toString());
            }
            def resultMap = results.toMap()
            log.debug("Successfully fetched new token")

            return resultMap.data.key
        } catch (Exception e) {
            log.error("Exception occurred during token provisioning: ${e.getMessage()}")
            throw new Exception("Exception occurred during token provisioning: ${e.getMessage()}")
        }
    }
}
