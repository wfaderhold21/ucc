
#include <arpa/inet.h>
#include <sys/stat.h>

struct pinger_attr {
	int npeers;
	struct sockaddr_storage sin;
	__be16 port;			/* dst port in NBO */
	int ping_interval_us;
};

struct pinger_peer_attr {
	struct sockaddr_storage sin;
	__be16 port;			/* dst port in NBO */
};

typedef void* pinger_t;			// pinger handle
typedef void* pinger_pid_t;		// peer id
typedef uint64_t pinger_rtt_t;	// RTT value

int pinger_create(struct pinger_attr *attr, pinger_t *pinger);
int pinger_destroy(pinger_t *pinger);
int pinger_connect(pinger_t pinger, struct pinger_peer_attr *attr, pinger_pid_t *peer);
int pinger_query(pinger_t pinger, pinger_pid_t peer, pinger_rtt_t *rtt);

