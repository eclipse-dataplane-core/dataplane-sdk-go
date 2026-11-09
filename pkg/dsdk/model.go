package dsdk

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	dataAddressType      = "DataAddress"
	endpointPropertyType = "EndpointProperty"
	typeKey              = "@type"
)

// DataAddress conveys information to access a wire protocol endpoint, serialized as defined by the Dataspace Protocol.
type DataAddress struct {
	EndpointType       string             `json:"endpointType" validate:"required"`
	Endpoint           string             `json:"endpoint,omitempty"`
	EndpointProperties []EndpointProperty `json:"endpointProperties,omitempty" validate:"omitempty,min=1,dive"`
}

// EndpointProperty is a name/value pair of a DataAddress.
type EndpointProperty struct {
	Name  string `json:"name" validate:"required"`
	Value string `json:"value"`
}

// Property returns the value of the endpoint property with the given name.
func (d *DataAddress) Property(name string) (string, bool) {
	if d == nil {
		return "", false
	}
	for _, p := range d.EndpointProperties {
		if p.Name == name {
			return p.Value, true
		}
	}
	return "", false
}

// Copy returns a deep copy of the data address, or nil if d is nil.
func (d *DataAddress) Copy() *DataAddress {
	if d == nil {
		return nil
	}
	c := *d
	if d.EndpointProperties != nil {
		c.EndpointProperties = append([]EndpointProperty(nil), d.EndpointProperties...)
	}
	return &c
}

type dataAddressJSON struct {
	Type               string                 `json:"@type"`
	EndpointType       string                 `json:"endpointType"`
	Endpoint           string                 `json:"endpoint,omitempty"`
	EndpointProperties []endpointPropertyJSON `json:"endpointProperties,omitempty"`
}

type endpointPropertyJSON struct {
	Type  string `json:"@type"`
	Name  string `json:"name"`
	Value string `json:"value"`
}

// MarshalJSON adds the JSON-LD @type values required by the DSP DataAddress schema.
func (d DataAddress) MarshalJSON() ([]byte, error) {
	out := dataAddressJSON{
		Type:         dataAddressType,
		EndpointType: d.EndpointType,
		Endpoint:     d.Endpoint,
	}
	for _, p := range d.EndpointProperties {
		out.EndpointProperties = append(out.EndpointProperties, endpointPropertyJSON{Type: endpointPropertyType, Name: p.Name, Value: p.Value})
	}
	return json.Marshal(out)
}

// UnmarshalJSON accepts a DSP DataAddress; the @type values are optional on input.
func (d *DataAddress) UnmarshalJSON(data []byte) error {
	var in dataAddressJSON
	if err := json.Unmarshal(data, &in); err != nil {
		return err
	}
	if in.Type != "" && in.Type != dataAddressType {
		return fmt.Errorf("invalid DataAddress %s: %q", typeKey, in.Type)
	}
	*d = DataAddress{EndpointType: in.EndpointType, Endpoint: in.Endpoint}
	for _, p := range in.EndpointProperties {
		if p.Type != "" && p.Type != endpointPropertyType {
			return fmt.Errorf("invalid EndpointProperty %s: %q", typeKey, p.Type)
		}
		d.EndpointProperties = append(d.EndpointProperties, EndpointProperty{Name: p.Name, Value: p.Value})
	}
	return nil
}

// DataAddressBuilder builds a DataAddress.
type DataAddressBuilder struct {
	address DataAddress
}

func NewDataAddressBuilder() *DataAddressBuilder {
	return &DataAddressBuilder{}
}

func (b *DataAddressBuilder) EndpointType(endpointType string) *DataAddressBuilder {
	b.address.EndpointType = endpointType
	return b
}

func (b *DataAddressBuilder) Endpoint(endpoint string) *DataAddressBuilder {
	b.address.Endpoint = endpoint
	return b
}

// Property adds an endpoint property.
func (b *DataAddressBuilder) Property(name string, value string) *DataAddressBuilder {
	b.address.EndpointProperties = append(b.address.EndpointProperties, EndpointProperty{Name: name, Value: value})
	return b
}

func (b *DataAddressBuilder) Build() (*DataAddress, error) {
	if b.address.EndpointType == "" {
		return nil, NewValidationError("endpointType is required")
	}
	for _, p := range b.address.EndpointProperties {
		if p.Name == "" {
			return nil, NewValidationError("endpoint property name is required")
		}
	}
	return b.address.Copy(), nil
}

// DataFlowState is the state of a data flow. It is stored as an int and serialized as the specification's string enum.
type DataFlowState int

const (
	Initialized DataFlowState = 0
	Preparing   DataFlowState = 50
	Prepared    DataFlowState = 100
	Starting    DataFlowState = 150
	Started     DataFlowState = 200
	Completed   DataFlowState = 250
	Suspended   DataFlowState = 300
	Terminated  DataFlowState = 350
)

var stateNames = map[DataFlowState]string{
	Initialized: "INITIALIZED",
	Preparing:   "PREPARING",
	Prepared:    "PREPARED",
	Starting:    "STARTING",
	Started:     "STARTED",
	Completed:   "COMPLETED",
	Suspended:   "SUSPENDED",
	Terminated:  "TERMINATED",
}

func (s DataFlowState) String() string {
	if name, ok := stateNames[s]; ok {
		return name
	}
	return fmt.Sprintf("UNKNOWN(%d)", int(s))
}

// ParseDataFlowState returns the state for the given specification name.
func ParseDataFlowState(name string) (DataFlowState, error) {
	for state, n := range stateNames {
		if n == name {
			return state, nil
		}
	}
	return 0, fmt.Errorf("%w: unknown data flow state %q", ErrInvalidInput, name)
}

func (s DataFlowState) MarshalJSON() ([]byte, error) {
	name, ok := stateNames[s]
	if !ok {
		return nil, fmt.Errorf("unknown data flow state %d", int(s))
	}
	return json.Marshal(name)
}

func (s *DataFlowState) UnmarshalJSON(data []byte) error {
	var name string
	if err := json.Unmarshal(data, &name); err != nil {
		return err
	}
	state, err := ParseDataFlowState(name)
	if err != nil {
		return err
	}
	*s = state
	return nil
}

// SuspendedBy records which party suspended a data flow. Only that party may resume it.
type SuspendedBy string

const (
	// SuspendedByNone is set when the flow is not suspended.
	SuspendedByNone SuspendedBy = ""
	// SuspendedByLocal is set when the data plane suspended the flow itself.
	SuspendedByLocal SuspendedBy = "local"
	// SuspendedByRemote is set when the control plane suspended the flow via the signaling API.
	SuspendedByRemote SuspendedBy = "remote"
)

type DataFlow struct {
	ID                     string
	Version                int64
	Consumer               bool
	AgreementID            string
	DatasetID              string
	RuntimeID              string
	UpdatedAt              int64
	CreatedAt              int64
	ParticipantID          string
	DataspaceContext       string
	CounterPartyID         string
	Profile                string
	Claims                 map[string]any
	Labels                 []string
	Metadata               map[string]any
	ControlPlaneID         string
	SuspendedBy            SuspendedBy
	State                  DataFlowState
	StateCount             uint
	StateTimestamp         int64
	SourceDataAddress      *DataAddress
	DestinationDataAddress *DataAddress
	ErrorDetail            string
}

func (df *DataFlow) profileSuffix() string {
	return df.Profile[strings.LastIndex(df.Profile, "-")+1:]
}

// IsPush returns true if the profile denotes a push transfer, i.e. its last '-' token is "push" (case-insensitive).
func (df *DataFlow) IsPush() bool {
	return strings.EqualFold(df.profileSuffix(), "push")
}

// IsPull returns true if the profile denotes a pull transfer, i.e. its last '-' token is "pull" (case-insensitive).
func (df *DataFlow) IsPull() bool {
	return strings.EqualFold(df.profileSuffix(), "pull")
}

// Copy returns a deep copy of the data flow. Claims and metadata values are copied shallowly.
func (df *DataFlow) Copy() *DataFlow {
	c := *df
	c.Claims = copyMap(df.Claims)
	c.Metadata = copyMap(df.Metadata)
	if df.Labels != nil {
		c.Labels = append([]string(nil), df.Labels...)
	}
	c.SourceDataAddress = df.SourceDataAddress.Copy()
	c.DestinationDataAddress = df.DestinationDataAddress.Copy()
	return &c
}

func copyMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	c := make(map[string]any, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

func (df *DataFlow) isFinal() bool {
	return df.State == Completed || df.State == Terminated
}

func (df *DataFlow) transition(state DataFlowState) {
	df.State = state
	df.StateTimestamp = time.Now().UnixMilli()
	df.StateCount++
}

func (df *DataFlow) invalidTransition(to DataFlowState) error {
	return fmt.Errorf("%w: cannot transition from %v to %v", ErrInvalidTransition, df.State, to)
}

func (df *DataFlow) TransitionToPreparing() error {
	if df.State == Preparing {
		return nil
	}
	if df.State != Initialized {
		return df.invalidTransition(Preparing)
	}
	df.transition(Preparing)
	return nil
}

func (df *DataFlow) TransitionToPrepared() error {
	if df.State == Prepared {
		return nil
	}
	if df.State != Initialized && df.State != Preparing {
		return df.invalidTransition(Prepared)
	}
	df.transition(Prepared)
	return nil
}

func (df *DataFlow) TransitionToStarting() error {
	if df.State == Starting {
		return nil
	}
	if df.State != Initialized && df.State != Prepared {
		return df.invalidTransition(Starting)
	}
	df.transition(Starting)
	return nil
}

// TransitionToStarted transitions a new, prepared or starting flow to STARTED. Use TransitionToResumed for a
// suspended flow.
func (df *DataFlow) TransitionToStarted() error {
	if df.State == Started {
		return nil
	}
	if df.State != Initialized && df.State != Prepared && df.State != Starting {
		return df.invalidTransition(Started)
	}
	df.transition(Started)
	return nil
}

// TransitionToSuspended transitions a STARTED flow to SUSPENDED and records who suspended it.
func (df *DataFlow) TransitionToSuspended(reason string, by SuspendedBy) error {
	if df.State == Suspended {
		return nil
	}
	if df.State != Started {
		return df.invalidTransition(Suspended)
	}
	if by != SuspendedByLocal && by != SuspendedByRemote {
		return fmt.Errorf("%w: invalid suspending party %q", ErrInvalidInput, by)
	}
	df.SuspendedBy = by
	df.ErrorDetail = reason
	df.transition(Suspended)
	return nil
}

// TransitionToResumed transitions a SUSPENDED flow back to STARTED. Only the party that suspended the flow may resume it.
func (df *DataFlow) TransitionToResumed(by SuspendedBy) error {
	if df.State != Suspended {
		return df.invalidTransition(Started)
	}
	if df.SuspendedBy != by {
		return fmt.Errorf("%w: data flow %s was suspended by the %s party and can only be resumed by it", ErrInvalidTransition, df.ID, df.SuspendedBy)
	}
	df.SuspendedBy = SuspendedByNone
	df.ErrorDetail = ""
	df.transition(Started)
	return nil
}

func (df *DataFlow) TransitionToCompleted() error {
	if df.State == Completed {
		return nil
	}
	if df.State != Started {
		return df.invalidTransition(Completed)
	}
	df.transition(Completed)
	return nil
}

// TransitionToTerminated transitions any non-final flow to TERMINATED. COMPLETED is final and cannot be terminated.
func (df *DataFlow) TransitionToTerminated(reason string) error {
	if df.State == Terminated {
		return nil
	}
	if df.State == Completed {
		return df.invalidTransition(Terminated)
	}
	df.ErrorDetail = reason
	df.transition(Terminated)
	return nil
}

type DataFlowBuilder struct {
	dataFlow DataFlow
}

func NewDataFlowBuilder() *DataFlowBuilder {
	return &DataFlowBuilder{}
}

func (b *DataFlowBuilder) ID(id string) *DataFlowBuilder {
	b.dataFlow.ID = id
	return b
}

func (b *DataFlowBuilder) Consumer(val bool) *DataFlowBuilder {
	b.dataFlow.Consumer = val
	return b
}

func (b *DataFlowBuilder) UpdatedAt(updatedAt int64) *DataFlowBuilder {
	b.dataFlow.UpdatedAt = updatedAt
	return b
}

func (b *DataFlowBuilder) CreatedAt(createdAt int64) *DataFlowBuilder {
	b.dataFlow.CreatedAt = createdAt
	return b
}

func (b *DataFlowBuilder) ParticipantID(id string) *DataFlowBuilder {
	b.dataFlow.ParticipantID = id
	return b
}

func (b *DataFlowBuilder) DataspaceContext(context string) *DataFlowBuilder {
	b.dataFlow.DataspaceContext = context
	return b
}

func (b *DataFlowBuilder) CounterpartyID(id string) *DataFlowBuilder {
	b.dataFlow.CounterPartyID = id
	return b
}

func (b *DataFlowBuilder) Profile(profile string) *DataFlowBuilder {
	b.dataFlow.Profile = profile
	return b
}

func (b *DataFlowBuilder) Claims(claims map[string]any) *DataFlowBuilder {
	b.dataFlow.Claims = claims
	return b
}

func (b *DataFlowBuilder) Labels(labels []string) *DataFlowBuilder {
	b.dataFlow.Labels = labels
	return b
}

func (b *DataFlowBuilder) Metadata(metadata map[string]any) *DataFlowBuilder {
	b.dataFlow.Metadata = metadata
	return b
}

func (b *DataFlowBuilder) ControlPlaneID(id string) *DataFlowBuilder {
	b.dataFlow.ControlPlaneID = id
	return b
}

func (b *DataFlowBuilder) SuspendedBy(by SuspendedBy) *DataFlowBuilder {
	b.dataFlow.SuspendedBy = by
	return b
}

func (b *DataFlowBuilder) State(state DataFlowState) *DataFlowBuilder {
	b.dataFlow.State = state
	return b
}

func (b *DataFlowBuilder) StateCount(count uint) *DataFlowBuilder {
	b.dataFlow.StateCount = count
	return b
}

func (b *DataFlowBuilder) StateTimestamp(timestamp int64) *DataFlowBuilder {
	b.dataFlow.StateTimestamp = timestamp
	return b
}

func (b *DataFlowBuilder) SourceDataAddress(address *DataAddress) *DataFlowBuilder {
	b.dataFlow.SourceDataAddress = address
	return b
}

func (b *DataFlowBuilder) DestinationDataAddress(address *DataAddress) *DataFlowBuilder {
	b.dataFlow.DestinationDataAddress = address
	return b
}

func (b *DataFlowBuilder) ErrorDetail(error string) *DataFlowBuilder {
	b.dataFlow.ErrorDetail = error
	return b
}

func (b *DataFlowBuilder) RuntimeID(id string) *DataFlowBuilder {
	b.dataFlow.RuntimeID = id
	return b
}

func (b *DataFlowBuilder) DatasetID(id string) *DataFlowBuilder {
	b.dataFlow.DatasetID = id
	return b
}

func (b *DataFlowBuilder) AgreementID(id string) *DataFlowBuilder {
	b.dataFlow.AgreementID = id
	return b
}

func (b *DataFlowBuilder) Build() (*DataFlow, error) {
	var validationErrs []string

	if b.dataFlow.CreatedAt == 0 {
		b.dataFlow.CreatedAt = time.Now().UnixMilli()
	}

	if b.dataFlow.UpdatedAt == 0 {
		b.dataFlow.UpdatedAt = b.dataFlow.CreatedAt
	}

	if b.dataFlow.StateTimestamp == 0 {
		b.dataFlow.StateTimestamp = b.dataFlow.CreatedAt
	}

	if b.dataFlow.ID == "" {
		validationErrs = append(validationErrs, "ID is required")
	}

	if b.dataFlow.ParticipantID == "" {
		validationErrs = append(validationErrs, "ParticipantID is required")
	}

	if b.dataFlow.DataspaceContext == "" {
		validationErrs = append(validationErrs, "DataspaceContext is required")
	}

	if b.dataFlow.CounterPartyID == "" {
		validationErrs = append(validationErrs, "CounterPartyID is required")
	}

	if b.dataFlow.Profile == "" {
		validationErrs = append(validationErrs, "Profile is required")
	}

	if len(validationErrs) > 0 {
		return nil, NewValidationError(validationErrs...)
	}

	return &b.dataFlow, nil
}
