package com.ipam.netbox

import com.morpheusdata.model.NetworkPoolType
import groovy.util.logging.Slf4j
import org.apache.commons.net.util.SubnetUtils
import java.nio.ByteBuffer


@Slf4j
class NetboxUtils {

    // Network calculation helper functions

    static String getPrefixLength(String ipWithCidr) {
        def parts = ipWithCidr?.split("/")
        return parts?.size() > 1 ? parts[1] : null
    }

    static int cidrToIPInt(String cidr) {
        def ip = cidr.split('/')[0]
        byte[] ipBytes = InetAddress.getByName(ip).address
        return ByteBuffer.wrap(ipBytes).int
    }


    static long cidrToLong(String cidr) {
        // Extract the IP part and prefix length from the CIDR
        def (ip, prefixLength) = cidr.tokenize('/')

        // Split the IP into octets
        def octets = ip.tokenize('.')

        // Convert the IP to an integer (actually, a long to avoid issues)
        long ipAsLong = 0
        for (octet in octets) {
            ipAsLong = (ipAsLong << 8) + octet.toLong()
        }

        // Shift the IP to the upper 32 bits and add the prefix length
        long result = (ipAsLong << 32) + prefixLength.toLong()

        return result
    }


    static int getIPCountBetween(String startIp, String endIp) {
        def startInt = cidrToIPInt(startIp)
        def endInt = cidrToIPInt(endIp)

        if (startInt > endInt) {
            throw new Exception("Start IP is greater than End IP.")
        }

        return endInt - startInt + 1
    }


    // get parent network by full address cidr
    // e.g. 192.168.1.31/24 will return 192.168.1.0/24
    static def getNetworkPrefix(String cidr) {
        def (ip, prefixLength) = cidr.split('/')
        byte[] ipBytes = InetAddress.getByName(ip).address
        int ipAsInt = ByteBuffer.wrap(ipBytes).int

        // Compute the subnet mask using the prefix length
        int mask = -1 << (32 - prefixLength.toInteger())

        // Calculate the network ID by bitwise AND-ing the IP with the subnet mask
        int networkIdAsInt = ipAsInt & mask

        byte[] networkIdBytes = ByteBuffer.allocate(4).putInt(networkIdAsInt).array()

        def networkID = InetAddress.getByAddress(networkIdBytes).hostAddress

        return "${networkID}/${prefixLength}"
    }


    static String getIPOnly(String cidr) {
        if (cidr == null) {
            return null
        }
        return cidr.split('/')[0]
    }


    static String cidrToSubnetMask(String cidr) {
        def parts = cidr.split("/")
        if (parts.length != 2) {
            return "Invalid CIDR format"
        }
        def prefixLength = Integer.parseInt(parts[1])
        if (prefixLength < 0 || prefixLength > 32) {
            return "Invalid prefix length"
        }

        // Calculate subnet mask from prefix length
        def mask = (0xFFFFFFFF << (32 - prefixLength)) & 0xFFFFFFFF
        def maskParts = []
        for (int i = 3; i >= 0; i--) {
            maskParts << String.valueOf((mask >> (i * 8)) & 0xFF)
        }

        return maskParts.join(".")
    }

    static boolean isIPInRange(String targetIP, String startIP, String endIP) {

        long startAddress = cidrToLong(startIP)
        long endAddress = cidrToLong(endIP)
        long targetAddress = cidrToLong(targetIP)

        return targetAddress >= startAddress && targetAddress <= endAddress
    }

    // Helper function to convert IP to long
    static long ipToLong(String ipAddress) {
        String[] octets = ipAddress.split("\\.");
        return (Long.parseLong(octets[0]) << 24) + (Integer.parseInt(octets[1]) << 16) +
                (Integer.parseInt(octets[2]) << 8) + Integer.parseInt(octets[3])
    }


    static Map generateNetworkPoolConfig(long poolServerId, Map networkEntity) {
        def cidr = networkEntity.isRange ? getNetworkPrefix(networkEntity.start_address) : networkEntity.prefix
        def rtn = [config:[:], ranges:[]]
        try {
            def subnetInfo = new SubnetUtils(cidr).getInfo()

            rtn.config.parentId = poolServerId
            rtn.config.parentType = 'NetworkPoolServer'
            rtn.config.poolEnabled = true
            rtn.config.configuration = networkEntity.site?.id ?: 0
            rtn.config.displayName = rtn.config.name
            rtn.config.type = new NetworkPoolType(code: networkEntity.typeCode)
            rtn.config.externalId = (int) networkEntity.id
            rtn.config.ipCount = 0
            rtn.config.cidr = cidr
            rtn.config.netmask = cidrToSubnetMask(cidr)
            if (networkEntity.isRange || !networkEntity.childRanges) { // no-parent range or subnet prefix without ranges
                if (networkEntity.isRange) {
                    def size = getIPCountBetween(networkEntity.start_address, networkEntity.end_address)
                    rtn.config.ipCount += size
                    rtn.config.name = networkEntity.isRange ? "${networkEntity.start_address} - ${networkEntity.end_address}" : cidr
                    rtn.ranges << [startAddress: getIPOnly(networkEntity.start_address), endAddress: getIPOnly(networkEntity.end_address)]
                } else {
                    def size = getIPCountBetween(subnetInfo.getLowAddress(), subnetInfo.getHighAddress())
                    rtn.config.ipCount += size
                    rtn.config.name = cidr
                    rtn.ranges << [startAddress: getIPOnly(subnetInfo.getLowAddress()), endAddress: getIPOnly(subnetInfo.getHighAddress()), addressCount: size]
                }
            } else {
                def subnetSize = getIPCountBetween(subnetInfo.getLowAddress(), subnetInfo.getHighAddress())
                rtn.config.ipCount = subnetSize
                rtn.config.name = cidr
                rtn.ranges << [startAddress: getIPOnly(subnetInfo.getLowAddress()), endAddress: getIPOnly(subnetInfo.getHighAddress()), addressCount: subnetSize]
                for (def range: networkEntity.childRanges) {
                    def size = getIPCountBetween(range.start_address, range.end_address)
                    //rtn.config.ipCount += size
                    rtn.ranges << [startAddress: getIPOnly(range.start_address), endAddress: getIPOnly(range.end_address), addressCount: size]
                }
            }
        } catch(e) {
            log.warn("error parsing network pool cidr: ${e}", e)
        }
        return rtn
    }
}
