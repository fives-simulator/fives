#include <random>
#include <cstdint>

#include "AllocationStrategy.h"

using namespace storalloc;

std::vector<std::shared_ptr<wrench::FileLocation>> storalloc::genericRRStrategy(
    const std::shared_ptr<wrench::DataFile> &file,
    const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources,
    const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
    const std::vector<std::shared_ptr<wrench::FileLocation>> &previous_allocations)
{

    // Init round-robin
    static auto last_selected_server = resources.begin()->first;
    static unsigned int internal_disk_selection = 0;
    auto capacity_req = file->getSize();
    std::shared_ptr<wrench::FileLocation> designated_location = nullptr;
    auto current = resources.find(last_selected_server);
    auto current_disk_selection = internal_disk_selection;

    auto continue_disk_loop = true;

    do
    {

        // std::cout << "Considering disk index " << std::to_string(current_disk_selection) << std::endl;
        unsigned int nb_of_local_disks = current->second.size();
        auto storage_service = current->second[current_disk_selection % nb_of_local_disks];
        // std::cout << "- Looking at storage service " << storage_service->getName() << std::endl;

        auto free_space = storage_service->getTotalFreeSpace();
        // std::cout << "- It has " << free_space << "B of free space" << std::endl;

        if (free_space >= capacity_req)
        {
            designated_location = wrench::FileLocation::LOCATION(std::shared_ptr<wrench::StorageService>(storage_service), file);
            // std::cout << "Chose server " << current->first << storage_service->getBaseRootPath() << std::endl;
            std::advance(current, 1);
            if (current == resources.end())
            {
                current = resources.begin();
                current_disk_selection++;
            }
            // Update for next function call
            last_selected_server = current->first;
            internal_disk_selection = current_disk_selection;
            // std::cout << "Next first server will be " << last_selected_server << std::endl;
            break;
        }

        std::advance(current, 1);
        if (current == resources.end())
        {
            current = resources.begin();
            current_disk_selection++;
        }
        if (current_disk_selection > (internal_disk_selection + nb_of_local_disks + 1))
        {
            // std::cout << "Stopping continue_disk_loop" << std::endl;
            continue_disk_loop = false;
        }
        // std::cout << "Next server will be " << current->first << std::endl;
    } while ((current->first != last_selected_server) or (continue_disk_loop));

    if (designated_location)
        // std::cout << "genericRRStrategy has completed" << std::endl;
        return std::vector<std::shared_ptr<wrench::FileLocation>>{designated_location};
    else
        // std::cout << "genericRRStrategy has completed with failure" << std::endl;
        return std::vector<std::shared_ptr<wrench::FileLocation>>();
}

LustreAllocationStrategy::ba_min_max LustreAllocationStrategy::lustreComputeMinMaxUtilization(const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources)
{

    // https://github.com/whamcloud/lustre/blob/a336d7c7c1cd62a5a5213835aa85b8eaa87b076a/lustre/obdclass/lu_tgt_descs.c#L488
    
    LustreAllocationStrategy::ba_min_max ba_min_max = {UINT64_MAX, 0};

    for (const auto &resource : resources)
    {
        for (const auto &service : resource.second)
        {
            auto ss_service = dynamic_pointer_cast<wrench::SimpleStorageService>(service);
            uint64_t current_free_space = ss_service->traceTotalFreeSpace();
            current_free_space >>= 8; // used in Lustre code to prevent overflows, we're blindly doing the same
            ba_min_max.min = min(current_free_space, ba_min_max.min);
            ba_min_max.max = max(current_free_space, ba_min_max.max);
        }
    }

    return ba_min_max;
}

bool LustreAllocationStrategy::lustreUseRR(struct ba_min_max ba_min_max)
{
    // Unlike in Lustre, we select the allocator based on avail bytes only, and not avail byte + avail inodes
    // This is because avail inodes is less relevant for us (no MDT model)
    // https://github.com/whamcloud/lustre/blob/a336d7c7c1cd62a5a5213835aa85b8eaa87b076a/lustre/obdclass/lu_tgt_descs.c#L587

    // Also note : >> 8 is equivalent to "/ 256" only more optimised, so this is equivelent to computing :
    // "is 83 % of 'max' greater than min"
    // (Thus leading to the determining if the imbalance between most and least used OST is greater than 17%)

    // Note : if your OSTs originally have != capacities, this will most likely not work as intended (but Lustre 
    // has other mechanisms to look into for such use cases)
    return (ba_min_max.max * (256 - LUSTRE_lq_threshold_rr)) >> 8 < ba_min_max.min;
}

/**
 * @brief Lustre-inspired allocation algorithm (round-robin with a few tweaks)
 *        meant to be used with the INTERNAL_STRIPING property of the CSS set to false
 *        (striping is done here).
 *
 */
std::vector<std::shared_ptr<wrench::FileLocation>> LustreAllocationStrategy::lustreStrategy(
    const std::shared_ptr<wrench::DataFile> &file,
    const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources,
    const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
    const std::vector<std::shared_ptr<wrench::FileLocation>> &previous_allocations)
{

    // Check for inbalance in the storage services utilization
    auto ba_min_max = lustreComputeMinMaxUtilization(resources);

    if (lustreUseRR(ba_min_max))
    {
        // Consider that every target has roughly the same free space, and use RR allocator
        std::cout << "[lustreStrategy] Using RR allocator" << std::endl;
        return lustreRRAllocator(file, resources, mapping, previous_allocations);
    }
    else
    {
        // Consider that targets free space use is too much imbalanced and go for the weighted allocator
        std::cout << "[lustreStrategy] Using weighted allocator" << std::endl;
        return lustreWeightedAllocator(file, resources, mapping, previous_allocations);
    }
}

std::vector<std::shared_ptr<wrench::StorageService>> LustreAllocationStrategy::lustreRROrderServices(const std::map<std::string, int> &hostname_to_service_count,
                                                                                      const std::vector<std::shared_ptr<wrench::StorageService>> &disk_level_services)
{

    std::vector<std::shared_ptr<wrench::StorageService>> rr_ordered_services(disk_level_services.size());
    fill(rr_ordered_services.begin(), rr_ordered_services.end(), nullptr);

    std::size_t placed_services = 0;

    for (const auto &server : hostname_to_service_count)
    {

        auto j = 0;
        auto srv_hostname = server.first;
        auto srv_service_count = server.second;

        const auto disk_level_svc_count = disk_level_services.size();
        for (std::size_t i = 0; i < disk_level_svc_count; i++)
        {

            int next;

            if (disk_level_services[i]->getHostname() != srv_hostname)
            {
                continue;
            }

            next = j * disk_level_svc_count / srv_service_count;
            while (rr_ordered_services[next] != nullptr)
            {
                next = (next + 1) % disk_level_svc_count;
            }

            rr_ordered_services[next] = disk_level_services[i];
            j++;
            placed_services++;
        }
    }

    if (placed_services != rr_ordered_services.size())
    {
        std::cout << "LustreAlloc: couldn't place all services in the round-robin pre-processed list (missing " << std::to_string(rr_ordered_services.size() - placed_services) << " services)" << std::endl;
        throw std::runtime_error("Number of placed services differs expected size of the RR-ordered services");
    }

    return rr_ordered_services;
}

LustreAllocationStrategy::striping LustreAllocationStrategy::lustreComputeStriping(double file_size_b, size_t number_of_OSTs)
{

    LustreAllocationStrategy::striping ret_striping = {};

    // How many stripes we need in total, considering total file size and default stripe_size.
    ret_striping.stripes_count = std::ceil(file_size_b / LUSTRE_stripe_size);
    ret_striping.stripes_per_ost = 1;

    if (file_size_b > LUSTRE_stripe_size)
    {
        if (ret_striping.stripes_count > number_of_OSTs)
        {
            while (ret_striping.stripes_count > number_of_OSTs *  ret_striping.stripes_per_ost)
            {
                 ret_striping.stripes_per_ost++; // here we don't ask for more storage than needed because the condition of the allocation loop
            }
        }
    }

    return ret_striping;
}

bool LustreAllocationStrategy::lustreOstIsUsed(const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping, const std::shared_ptr<wrench::StorageService> ost)
{

    for (const auto &alloc_mapping : mapping)
    {
        auto location_vector = alloc_mapping.second;
        for (const auto &location : location_vector)
        {
            if (location->getStorageService()->getHostname() == ost->getHostname())
            {
                return true;
            }
        }
    }

    return false;
}

std::vector<std::shared_ptr<wrench::FileLocation>> LustreAllocationStrategy::lustreCreateFileParts(const std::string &file_id, std::map<int, std::shared_ptr<wrench::StorageService>> temp_allocations)
{

    std::vector<std::shared_ptr<wrench::FileLocation>> designated_locations = {};

    for (const auto &stripe_entry : temp_allocations)
    {
        auto part = wrench::Simulation::addFile(file_id + "_part_" + std::to_string(stripe_entry.first), LUSTRE_stripe_size);
        designated_locations.push_back(
            wrench::FileLocation::LOCATION(
                std::shared_ptr<wrench::StorageService>(stripe_entry.second), part));
    }

    return designated_locations;
}

std::vector<std::shared_ptr<wrench::FileLocation>> LustreAllocationStrategy::lustreRRAllocator(
    const std::shared_ptr<wrench::DataFile> &file,
    const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources,
    const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
    const std::vector<std::shared_ptr<wrench::FileLocation>> &previous_allocations)
{

    // 1. Prepare an ordered list of disk-level services (~OST) from all resources (~OSS)
    //    Roughly equivalent to lod_qos.c::lod_qos_calc_rr() in Lustre sources.
    std::map<std::string, int> hostname_to_service_count;                     // OSS
    std::vector<std::shared_ptr<wrench::StorageService>> disk_level_services; // OST
    for (const auto &host : resources)
    { // Flatten OST list
        disk_level_services.insert(disk_level_services.end(), host.second.begin(), host.second.end());
        hostname_to_service_count[host.first] = host.second.size();
    }
    auto rr_ordered_services = lustreRROrderServices(hostname_to_service_count, disk_level_services);
    auto rr_services_count = rr_ordered_services.size();

    // 2. Use the rr ordered list of services to allocation stripes for the given file
    // Roughly the equivalent of lod_qos.c::lod_ost_alloc_rr()

    /** Note: In Lustre, the user provides the striping pattern (stripe size and number of OSTs onto which the stripes will be placed)
     *  Here we don't have (nor want) this kind of user input, so we need to derive the stripe size and number of used OSTs from the file
     *  size and the total number of OSTs.
     */
    auto file_size_b = file->getSize();
    // auto stripe_count = std::ceil(file_size_b / LUSTRE_stripe_size); // Here we potentially ask for more storage than needed due to the rounding
    auto current_striping = lustreComputeStriping(file_size_b, rr_services_count);

    // Randomly chosen index of first OST of the stripe pattern
    // (from https://en.cppreference.com/w/cpp/numeric/random/uniform_int_distribution)
    std::random_device rd;
    std::mt19937 gen(rd()); // replace rd() by a static seed for reproducibility
    std::uniform_int_distribution<> distrib(0, rr_services_count - 1);
    int start_ost_index = distrib(gen); // similar to when Lustre chooses the first OST to be used.
    int temp_start_ost_index = start_ost_index;
    int stripe_idx = 0;

    std::map<int, std::shared_ptr<wrench::StorageService>> temp_stripe_locations;

    // Some sort of retry counter (same name as in Lustre but I don't know why they call it 'speed')
    for (auto speed = 0; speed < 2; speed++)
    {
        for (size_t i = 0; i < rr_services_count * current_striping.stripes_per_ost && stripe_idx < current_striping.stripes_count; i++)
        {

            auto array_idx = temp_start_ost_index % rr_services_count;
            ++temp_start_ost_index;
            auto current_ost = rr_ordered_services[array_idx];

            if (current_ost->traceTotalFreeSpace() < LUSTRE_stripe_size or (current_ost->getState() != wrench::S4U_Daemon::State::UP))
            {
                // Only keep running storage services associated with non-full disks
                std::cout << "LustreAlloc: skipping OST (total Free space == " << std::to_string(current_ost->traceTotalFreeSpace()) << " and current state == " << current_ost->getState() << ")" << std::endl;
                continue;
            }

            // For the first iteration, avoid services which already have an older allocation on them.
            if (speed == 0 && lustreOstIsUsed(mapping, current_ost))
            {
                std::cout << "LustreAlloc : Skipping OST because another allocation is already using it" << std::endl;
                continue;
            }
            else
            {
                std::cout << "LustreAlloc : Using OST " << current_ost->getHostname() << std::endl;
                temp_stripe_locations[stripe_idx] = current_ost;
                stripe_idx++;
            }
        }

        if (stripe_idx == current_striping.stripes_count)
        {
            std::cout << "LustreAlloc : All stripes allocated/ Stopping" << std::endl;
            break;
        }
        else
        {
            temp_stripe_locations.clear();
            temp_start_ost_index = start_ost_index; // back to original index, but this time we'll allow 'slow' OSTs
            stripe_idx = 0;
            std::cout << "LustreAlloc : Starting over because stripe_idx != stripe_count and we reached the condition of this loop" << std::endl;
        }
    }

    // If we could allocate every stripe, return an empty vector that will be interpreted as an allocation failure
    if (temp_stripe_locations.size() < current_striping.stripes_count)
    {
        std::cout << "LustreAlloc: Expected to allocate " << std::to_string(current_striping.stripes_count) << " stripes, but could only allocate " << std::to_string(temp_stripe_locations.size()) << " instead" << std::endl;
    }

    auto designated_locations = lustreCreateFileParts(file->getID(), temp_stripe_locations);
    return designated_locations;
}

uint64_t LustreAllocationStrategy::lustreComputeOstPenalty(uint64_t free_space_b, uint64_t used_inode_count, double active_service_count)
{

    // https://github.com/whamcloud/lustre/blob/a336d7c7c1cd62a5a5213835aa85b8eaa87b076a/lustre/obdclass/lu_tgt_descs.c#L532
  
    if (active_service_count == 0) {
        throw std::runtime_error("lustreComputeOstPenalty: active_service_count cannot be = 0");
    }

    free_space_b >>= 16; // Bitshift for overflow
    uint64_t free_inode_count = (LUSTRE_max_inodes - used_inode_count);
    free_inode_count >>= 8;
    uint64_t penalty = ((LUSTRE_prio_wide * free_space_b * free_inode_count) >> 8);
    penalty /= active_service_count;
    penalty >>= 1;
    return penalty;
}

uint64_t LustreAllocationStrategy::lustreComputeOstWeight(uint64_t free_space_b, uint64_t used_inode_count, uint64_t ost_penalty)
{

    // https://github.com/whamcloud/lustre/blob/a336d7c7c1cd62a5a5213835aa85b8eaa87b076a/lustre/obdclass/lu_tgt_descs.c#L232

    uint64_t weight = (free_space_b >> 16) * ((LUSTRE_max_inodes - used_inode_count) >> 8);

    if (weight < ost_penalty)
        return 0;
    else
        return (weight - ost_penalty);
}


uint64_t LustreAllocationStrategy::lustreComputeOssPenalty(uint64_t free_space_b, uint64_t free_inode_count, size_t ost_count, size_t oss_count)
{

    // https://github.com/whamcloud/lustre/blob/a336d7c7c1cd62a5a5213835aa85b8eaa87b076a/lustre/obdclass/lu_tgt_descs.c#L563
    /** Per server penalty:
     *  prio * current__srv_free_space * current_srv_free_inodes / number of targets in server / (number of server - 1) / 2
     */

    if (ost_count == 0 or oss_count == 0) {
        throw std::runtime_error("lustreComputeOstPenalty: active_service_count cannot be = 0");
    }

    auto oss_penalty = (LUSTRE_prio_wide * free_space_b * free_inode_count) >> 8;
    oss_penalty /= (ost_count * oss_count);
    oss_penalty >>= 1;
    return oss_penalty;
}

/**
 * @brief Lustre-inspired allocation algorithm (round-robin with a few tweaks)
 *        meant to be used with the INTERNAL_STRIPING property of the CSS set to false
 *        (striping is done here).
 *
 */
std::vector<std::shared_ptr<wrench::FileLocation>> LustreAllocationStrategy::lustreWeightedAllocator(
    const std::shared_ptr<wrench::DataFile> &file,
    const std::map<std::string, std::vector<std::shared_ptr<wrench::StorageService>>> &resources,
    const std::map<std::shared_ptr<wrench::DataFile>, std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
    const std::vector<std::shared_ptr<wrench::FileLocation>> &previous_allocations)
{

    // 1. For all services we need to calculate their weight.

    /** TODO: this is probably incorrect  : num_active_service should be the number of
     *  ACTIVE targets (i.e. with a current allocation), not the entire number of targets
     *  but so far we'll consider that every target is active at any time.
     *
     *  Also /!\ this variable is used throughout the function
     */
    auto active_ost_count = 0;
    for (const auto &resource : resources)
    {
        active_ost_count += resource.second.size();
    }

    uint64_t total_weight = 0;
    std::map<std::shared_ptr<wrench::StorageService>, uint64_t> weighted_services = {};

    for (const auto &resource : resources)
    {

        auto srv_free_space = 0;
        auto srv_free_inodes = 0;
        std::map<std::shared_ptr<wrench::StorageService>, uint64_t> weighted_oss_services = {};

        for (const auto &service : resource.second)
        {

            auto ss_service = dynamic_pointer_cast<wrench::SimpleStorageService>(service);
            uint64_t current_free_space = ss_service->traceTotalFreeSpace();
            srv_free_space += (current_free_space >> 16);                  // Bitshift for overflow
            uint64_t current_files = ss_service->traceTotalFiles();
            srv_free_inodes += ((LUSTRE_max_inodes - current_files) >> 8); // Bitshift for overflow, once again

            auto ost_penalty = lustreComputeOstPenalty(current_free_space, current_files, active_ost_count);
            auto weight = lustreComputeOstWeight(current_free_space, current_files, ost_penalty);

            // At this point we should also take into account for how long the OST has been IDLE.
            // So far we don't because... it's very complex to get it right in the simulation.

            weighted_oss_services[service] = weight;
        }

        // At this point we should also take into account for how long the OSS has been IDLE.
        // So far we don't because... it's very complex to get it right in the simulation.

        // Update all weights with server penalty
        auto oss_penalty = lustreComputeOssPenalty(srv_free_space, srv_free_inodes, weighted_oss_services.size(), resources.size());

        for (auto &oss_service : weighted_oss_services)
        {
            if (oss_service.second < oss_penalty)
                oss_service.second = 0;
            else
                oss_service.second -= oss_penalty;

            total_weight += oss_service.second;
        }

        // Update global weight map
        weighted_services.insert(weighted_oss_services.begin(), weighted_oss_services.end());
    }

    // 2. Pick OST at random, but favor larger weights
    // https://github.com/whamcloud/lustre/blob/a336d7c7c1cd62a5a5213835aa85b8eaa87b076a/lustre/lod/lod_qos.c#L1503

    /** In Lustre, the user provides a striping pattern (stripe size and number of OSTs onto which the stripes will be placed)
     *  Here we don't have (nor want) this kind of user input, so we need to derive the stripe size and number of used OSTs from the file
     *  size and the total number of OSTs, with some homemade rule.
     */
    auto file_size_b = file->getSize();
    auto stripe_count = std::ceil(file_size_b / LUSTRE_stripe_size); // Here we potentially ask for more storage than needed due to the rounding
    // auto stripes_per_ost = lustreComputeStripesPerOST(file_size_b, LUSTRE_stripe_size, active_ost_count, stripe_count);

    std::cout << "LUSTRE WEIGHT ALLOC DEBUG" << std::endl;
    // std::cout << "Stripe per ost = " << std::to_string(stripes_per_ost) << std::endl;
    std::cout << "Stripe count = " << std::to_string(stripe_count) << std::endl;
    std::cout << "Current file_size = " << std::to_string(file_size_b) << std::endl;
    std::cout << "Number of OST / services = " << std::to_string(active_ost_count) << std::endl;

    // from https://en.cppreference.com/w/cpp/numeric/random/uniform_int_distribution
    std::random_device rd;
    std::mt19937 gen(rd()); // replace rd() by a static seed for reproducibility
    std::uniform_int_distribution<> distrib(0, total_weight);

    std::map<int, std::shared_ptr<wrench::StorageService>> temp_stripe_locations;

    // Let's find enough OSTs for our allocation
    auto nfound = 0;
    while (nfound < stripe_count)
    {
        uint64_t rand, cur_weight;

        cur_weight = 0;
        rand = distrib(gen);

        auto stripe_idx = 0;
        for (const auto &service : weighted_services)
        {

            cur_weight += service.second;
            if (cur_weight < rand)
                continue;

            temp_stripe_locations[nfound] = service.first;
            nfound++;
        }
    }

    // If we could allocate every stripe, return an empty vector that will be interpreted as an allocation failure
    if (temp_stripe_locations.size() < stripe_count)
    {
        std::cout << "LustreAlloc: Expected to allocate " << std::to_string(stripe_count) << " stripes, but could only allocate " << std::to_string(temp_stripe_locations.size()) << " instead" << std::endl;
    }

    auto designated_locations = lustreCreateFileParts(file->getID(), temp_stripe_locations);
    return designated_locations;
}




// "Magic" default Lustre settings, that equals to computing whether or not the free space
// diff between OSTs is greater thant 17% or not
static constexpr uint64_t LUSTRE_lq_threshold_rr = 43;
// In Lustre, this priority represents how important 
// is free space compared to using a wide array of targets
// Here we use the default priority, which balanced towards free space (91%)
static constexpr uint64_t LUSTRE_lq_prio_free = 232;
static constexpr uint64_t LUSTRE_prio_wide = 24; // 256 - 232
static constexpr uint64_t LUSTRE_max_nb_ost = 2000;    // never stripe on more than 2000 OSTs, that's the Lustre limit when using ZFS.
    
/** Currently, the stripe size is arbitrarily set to 512MB (recommended size is between 1-4MB, and max is 4GB, but anyway, our allocations
 *  do not necessarily represent files...)
 */
static uint64_t LUSTRE_stripe_size;   // how much data is written to a given OST before moving to the next one (512MB in this case)

static constexpr uint64_t LUSTRE_max_inodes = (1ULL << 32);     // Approximate number of inodes on Linux