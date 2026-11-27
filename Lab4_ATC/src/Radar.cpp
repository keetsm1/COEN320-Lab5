#include "Radar.h"
#include <sys/dispatch.h>

#define SHARED_MEMORY_NAME "/gunkeet_tracy_radar_shm"

Radar::Radar(uint64_t& tick_counter) : tick_counter_ref(tick_counter), activeBufferIndex(0), timer(1,0), stopThreads(false) {
	clearSharedMemory(); //For future Use
	// Start threads for listening to airspace events
    Arrival_Departure = std::thread(&Radar::ListenAirspaceArrivalAndDeparture, this);
    UpdatePosition = std::thread(&Radar::ListenUpdatePosition, this);
    Radar_channel = NULL;
}

Radar::~Radar() {
    // Join threads to ensure proper cleanup
    shutdown();
    clearSharedMemory();//For future Use */
}

void Radar::shutdown() {
    // Set stop flag and wait for threads to complete
    stopThreads.store(true);

    // If the channel exists, close it properly
    if (Radar_channel) {
        name_detach(Radar_channel, 0);
    }

    if (Arrival_Departure.joinable()) {
        Arrival_Departure.join();
    }
    if (UpdatePosition.joinable()) {
        UpdatePosition.join();
    }
}


// Method to get the current active buffer
std::vector<msg_plane_info>& Radar::getActiveBuffer() {
    return planesInAirspaceData[activeBufferIndex];
}
//Coen320_Lab4 (Task0): Create channel to be reachable by radar that wants to poll the Airplane
//Radar Channel name should contain your group name
//To choose the channel with concatenating your group name with "Radar"
//Note: It is critical to not interfere with other groups
void Radar::ListenAirspaceArrivalAndDeparture() {
	Radar_channel = name_attach(NULL, "gunkeet", 0);
	if (Radar_channel == NULL) {
		std::cerr << "Failed to create channel for Radar" << std::endl;
		exit(EXIT_FAILURE);
	}
	// Simulated listening for aircraft arrivals and departures
    while (!stopThreads.load()) {
        // Replace with IPC
        Message msg;
        int rcvid = MsgReceive(Radar_channel->chid, &msg, sizeof(msg), nullptr); // Replace with actual channel ID
        if (rcvid == -1) {
        	// Silently skip if MsgReceive fails, but no crash happens
        	// std::cerr << "Error receiving airspace message:" << strerror(errno) << std::endl;
        	continue;
        }

        // Reply back to the client
        int msg_ret = msg.planeID;
        MsgReply(rcvid, 0, &msg_ret, sizeof(msg_ret)); // Send plane's ID back to airplane

        switch (msg.type) {
        case MessageType::ENTER_AIRSPACE:
            addPlaneToAirspace(msg);
            break;
        case MessageType::EXIT_AIRSPACE:
            removePlaneFromAirspace(msg.planeID);
            break;
        default:
        	//All other messages dropped
            //std::cerr << "Unknown airspace message type" << std::endl;
        	break;
        }

    }
}

void Radar::ListenUpdatePosition() {

    while (!stopThreads.load()) {
    	timer.waitTimer(); // Wait for the next timer interval before polling again
    	// Only poll airspace if there are planes
        if (!planesInAirspace.empty()) {
            pollAirspace();  // Call pollAirspace() to gather position data
            writeToSharedMemory();  // Write active buffer to shared memory //For future Use
            wasAirspaceEmpty = false;
        } else if (!wasAirspaceEmpty){
        	// Only write empty buffer once after transition to empty
        	writeToSharedMemory();  // Write to shared mem when all planes have left the airspace //For future Use
        	wasAirspaceEmpty = true;  // Set flag to indicate airspace is empty
        } else{
        	//std::cout << "Airspace is empty\n";
        }

    }
}

void Radar::pollAirspace(){

	airspaceMutex.lock();
	// Make a copy of the current planes in airspace to avoid modification during iteration
	std::unordered_set<int> planesToPoll = planesInAirspace;
	airspaceMutex.unlock();

	int inactiveBufferIndex = (activeBufferIndex + 1) % 2;
	std::vector<msg_plane_info>& inactiveBuffer = planesInAirspaceData[inactiveBufferIndex];
	inactiveBuffer.clear();


	//make channel to aircraft
	for (int planeID: planesToPoll){

		airspaceMutex.lock();
		bool isPlaneInAirspace = planesInAirspace.find(planeID) != planesInAirspace.end();
		airspaceMutex.unlock();
		if (isPlaneInAirspace){
			try {
			// Confirm that the plane is still in airspace
				msg_plane_info plane_info = getAircraftData(planeID);
				inactiveBuffer.emplace_back(plane_info);
			} catch (const std::exception& e) {
				// if error to process plane get next id and exception description
				//std::cerr << "Radar: Failed to get plane data " << planeID << ": " << e.what() << "\n";
				continue;
			}
		}


		{
			std::lock_guard<std::mutex> lock(bufferSwitchMutex);
		    activeBufferIndex = inactiveBufferIndex;
		}
	}
}

msg_plane_info Radar::getAircraftData(int id) {
	//Coen320_Lab4 (Task0): You need to correct the channel name
	//You may use your group name + plane id

	std::string id_str = "gunkeet"+std::to_string(id);  // Convert integer id to string
	const char* ID = id_str.c_str();         // Convert string to const char*
	int plane_channel = name_open(ID, 0);

	if (plane_channel == -1) {
		throw std::runtime_error("Radar: Error occurred while attaching to channel");
	}

	// Prepare a message to request position data
	Message requestMsg;
	requestMsg.type = MessageType::REQUEST_POSITION;
	requestMsg.planeID = id;
	requestMsg.data = NULL;

	// Structure to hold the received position data
	Message receiveMessage;

	// Send the position request to the aircraft and receive the response
	if (MsgSend(plane_channel, &requestMsg, sizeof(requestMsg), &receiveMessage, sizeof(receiveMessage)) == -1) {
		name_close(plane_channel);
		throw std::runtime_error("Radar: Error occurred while sending request message to aircraft");
	}

	msg_plane_info received_info = *static_cast<msg_plane_info*>(receiveMessage.data);

	// Close the communication channel with the aircraft
	name_close(plane_channel);

	return received_info;
}

void Radar::addPlaneToAirspace(Message msg) {
	std::lock_guard<std::mutex> lock(airspaceMutex);
	int plane_data = msg.planeID;
    planesInAirspace.insert(plane_data);
    std::cout << "Plane " << msg.planeID << " added to airspace" << std::endl;
}

void Radar::removePlaneFromAirspace(int planeID) {
	std::lock_guard<std::mutex> lock(airspaceMutex);
	planesInAirspace.erase(planeID);  // Directly remove the integer from the list
	std::cout << "Plane " << planeID << " removed from airspace" << std::endl;
}


void Radar::writeToSharedMemory() {
	// COEN320 Lab4_5 Task 2.1
	/*
	You need to implement the shared memory writing process here
	Save SharedMemory structure to shared memory
	Make sure to use mutex to protect the buffer switching process (e.g., bufferSwitchMutex)
	To store aircraft data, use inactiveBuffer which is obtained from getActiveBuffer() method
	Check the code snippet below for reference
	at the end you need to unmap the shared memory and close the file descriptor
	e.g., munmap(<SharedMemory object, shared_mem>, SHARED_MEMORY_SIZE) and close(shm_fd)


	// Get the active buffer based on the current active index
    std::vector<msg_plane_info>& activeBuffer = getActiveBuffer();
    // Get the current timestamp
    shared_mem->timestamp = tick_counter_ref;

	// Check if activeBuffer is empty and set the flag accordingly
	if (activeBuffer.empty()) {
		std::vector<msg_plane_info>& inactiveBuffer = planesInAirspaceData[(activeBufferIndex + 1) % 2];
		if (!inactiveBuffer.empty()){
			shared_mem->is_empty.store(false);
			shared_mem->count = inactiveBuffer.size();
			std::memcpy(shared_mem->plane_data, inactiveBuffer.data(), inactiveBuffer.size() * sizeof(msg_plane_info));
			inactiveBuffer.clear();
		}
		shared_mem->is_empty.store(true);
		shared_mem->count = 0;  // No planes
	} else {
            shared_mem->is_empty.store(false);
            shared_mem->count = activeBuffer.size();
            std::memcpy(shared_mem->plane_data, activeBuffer.data(), activeBuffer.size() * sizeof(msg_plane_info));
            activeBuffer.clear();
        }
	// clean up

	*/
	
	//create or open shared mem:
	// shared_mem_name-> unique name of shared memory, O_CREATE  =create if it doesnt exist, O_RDWR = ALLOWS reading and writting
	//0666 = read and write permissions for users subjet to processes permission mask
	int shm_fd = shm_open(SHARED_MEMORY_NAME, O_CREAT | O_RDWR, 0666);

	//if fails print error and exit
	if (shm_fd == -1){
		perror("shm_open");
		return;
	}

	//creating shared mem does not automatially give it size, ftruncate() sets the size of the shared memory so it has enough space to store
	//the sharedmemory structuture.
	if(ftruncate(shm_fd, SHARED_MEMORY_SIZE) == -1){
		perror("ftruncate");
		close(shm_fd);
		return;
	}

	//mmap allows program to access shared memory if it was like a normal variable in the code
	SharedMemory* shared_mem = static_cast<SharedMemory*>(
			//null_ptr lets qnx choose mem address, shared_memory_size -> memory size, prot_read/write-> allows reading and writing
			//MAP_SHARED makes it so changes are visible to other process mapping same memory
			//shm_fd which shared memory object to mom, 0 means start the beginning of memory.
			mmap(null_ptr, SHARED_MEMORY_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd,0)
			);

	if (shared_mem == MAP_FAILED){
		perror("mmap");
		close(shmd_fd);
		return;
	}

	{
		//locks so that another thread cannot switch it whilst copying ac data mutex unlocks after }s
		std::lock_guard<std::mutex> lock(bufferSwitchMutex);

		//getActiveBuffer() gets the buffer containing the latest ac info
		//timestamp stores the simulation time in shared memory and the & means we use the og buffer instead of making a copy.
		std::vector<msg_plane_info>& activeBuffer = getActiveBuffer();

		shared_mem->timestamp = tick_counter_ref;

		//check if there is any aircraft
		if(activeBuffer.empty()){
			//if empty tells display no ac
			shared_mem-> count = 0;
			shared_mem-> is_empty.store(true);
		}else{
			//if buffer not empty-> store the number of ac in shared mem
			shared_mem-> count = activeBuffer.size();

			//this copies ac data into shared memory
			std::memcpy(shared_mem-> plane_data, activateBuffer.data(), activeBuffer.size() * sizeof(msg_plane_info));
			//plane_data where to copy, activateBuffer.data()-> what to copy, activeBuffer.size() * sizeof(msg_plane_info) -> how many bytes to copy

			//tels disp data is available
			shared_mem->is_empty.store(false);
			//empty og bufer because done copying.
			activeBuffer.clear();
		}
	}

	//unmap shared mem since it has finished writing does not dleeted shared mem
	if(mummap(shared_mem, SHARED_MEMORY_SIZE) == -1){
		perror("munmap");
	}

	//close file description
	if(close(shm_fd) == -1){
		perror("close");
	}

}

void Radar::clearSharedMemory() {
	// COEN320 Lab4_5 Task 2.2
	// You need to implement the shared memory clearing process here
	// Initialize the shared memory structure to an empty state
	// You need to clear plane_data, count, etc.
	// e.g.
	//std::memset(sharedMemPtr->plane_data, 0, sizeof(sharedMemPtr->plane_data));  // Clear plane data
	//sharedMemPtr->count = 0;  // No planes initially
	//sharedMemPtr->is_empty.store(true);  // Set the is_empty flag to false to not block reader
	// Finally unmap the shared memory and close the file descriptor
	
}
